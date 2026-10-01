#!/usr/bin/env python3
"""Build the pinned official CUBRIDdb driver used as the differential oracle (#446).

The official driver (``CUBRIDdb`` wrapper over the ``_cubrid`` C extension) is
built from exact, verified source identities, never from PyPI (which only has
9.x source archives) or a locally installed build:

* ``CUBRID/cubrid-python`` at ``CUBRID_PYTHON_COMMIT``;
* its ``cci-src`` gitlink, which must equal ``CCI_COMMIT``.

CCI is built with CMake directly (target ``cascci_static``, the default bundled
OpenSSL 1.1.1f static libraries tracked in the CCI tree); the upstream
``setup.py``/``build_cci.sh`` wrapper is not run and no upstream file is
patched. The extension is compiled from the unchanged
``cubrid_ext/python_cubrid.c``; only the generated ``version.h`` (a template
upstream) is written outside the source tree.

The output directory is an importable root (``PYTHONPATH=<out>``) containing
``CUBRIDdb/``, ``_cubrid<EXT_SUFFIX>`` and ``oracle.json`` recording the source
commits, toolchain and the extension SHA-256. ``--verify`` re-checks a cached
output: the manifest pins, the Python version and the extension hash.

Usage:
    python scripts/build_official_oracle.py --out .official-oracle [--work DIR]
    python scripts/build_official_oracle.py --out .official-oracle --verify

Requires git, cmake (>= 3.21), a C/C++ compiler and the running interpreter's
development headers. Linux x86_64 only, like the upstream build.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import shutil
import subprocess  # nosec B404 - fixed argv lists, no shell
import sys
import sysconfig
from pathlib import Path

CUBRID_PYTHON_REPO = "https://github.com/CUBRID/cubrid-python.git"
CUBRID_PYTHON_COMMIT = "e75ec36b2a92b8829a49a967a29a1fbb9d7c322b"
CCI_REPO = "https://github.com/CUBRID/cubrid-cci.git"
CCI_COMMIT = "7d1eb8f40f04089b8218d08e36e2c24a2de11b24"
MANIFEST = "oracle.json"


def _run(argv: list[str], cwd: Path | None = None) -> str:
    print("+", " ".join(argv), flush=True)
    done = subprocess.run(  # nosec B603 - fixed argv lists, no shell
        argv, cwd=cwd, check=True, text=True, stdout=subprocess.PIPE
    )
    return done.stdout.strip()


def _checkout(repo: str, commit: str, dest: Path) -> None:
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    _run(["git", "init", "-q"], cwd=dest)
    _run(["git", "fetch", "-q", "--depth", "1", repo, commit], cwd=dest)
    _run(["git", "checkout", "-q", "--detach", "FETCH_HEAD"], cwd=dest)
    head = _run(["git", "rev-parse", "HEAD"], cwd=dest)
    if head != commit:
        raise SystemExit(f"{repo}: checked out {head}, expected {commit}")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _extension_name() -> str:
    return "_cubrid" + str(sysconfig.get_config_var("EXT_SUFFIX"))


def build(out: Path, work: Path) -> dict[str, object]:
    if platform.system() != "Linux" or platform.machine() != "x86_64":
        raise SystemExit("the official oracle build supports Linux x86_64 only")
    source = work / "cubrid-python"
    _checkout(CUBRID_PYTHON_REPO, CUBRID_PYTHON_COMMIT, source)
    gitlink = _run(["git", "ls-tree", "HEAD", "cci-src"], cwd=source).split()
    if len(gitlink) < 3 or gitlink[1] != "commit" or gitlink[2] != CCI_COMMIT:
        raise SystemExit(f"cci-src gitlink {gitlink!r} does not pin {CCI_COMMIT}")
    cci = source / "cci-src"
    _checkout(CCI_REPO, CCI_COMMIT, cci)

    cci_build = work / "cci-build"
    if cci_build.exists():
        shutil.rmtree(cci_build)
    _run(["cmake", "-S", str(cci), "-B", str(cci_build), "-DCMAKE_BUILD_TYPE=Release"])
    _run(["cmake", "--build", str(cci_build), "--target", "cascci_static", "--parallel"])
    static_lib = cci_build / "cci" / "libcascci.a"
    if not static_lib.is_file():
        raise SystemExit(f"CCI static library missing: {static_lib}")

    version = (source / "VERSION").read_text(encoding="utf-8").splitlines()[0].strip()
    driver_version = f"{version}+{CUBRID_PYTHON_COMMIT[:7]}"
    generated = work / "generated"
    generated.mkdir(parents=True, exist_ok=True)
    template = (source / "cubrid_ext" / "version.h.template").read_text(encoding="utf-8")
    (generated / "version.h").write_text(
        template.replace("{{VERSION}}", driver_version), encoding="utf-8"
    )

    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    extension = out / _extension_name()
    compiler = str(sysconfig.get_config_var("CC") or "cc").split()[0]
    _run(
        [
            compiler,
            "-shared",
            "-fPIC",
            "-O2",
            f"-I{generated}",
            f"-I{sysconfig.get_paths()['include']}",
            f"-I{cci / 'src' / 'base'}",
            f"-I{cci / 'src' / 'cci'}",
            str(source / "cubrid_ext" / "python_cubrid.c"),
            str(static_lib),
            f"-L{cci / 'external' / 'openssl' / 'lib'}",
            "-lssl",
            "-lcrypto",
            "-lpthread",
            "-lstdc++",
            "-o",
            str(extension),
        ]
    )
    shutil.copytree(source / "CUBRIDdb", out / "CUBRIDdb")

    manifest: dict[str, object] = {
        "cubrid_python_repository": CUBRID_PYTHON_REPO,
        "cubrid_python_commit": CUBRID_PYTHON_COMMIT,
        "cci_repository": CCI_REPO,
        "cci_commit": CCI_COMMIT,
        "driver_version": driver_version,
        "python": platform.python_version(),
        "compiler": _run([compiler, "--version"]).splitlines()[0],
        "cmake": _run(["cmake", "--version"]).splitlines()[0],
        "extension": extension.name,
        "extension_sha256": _sha256(extension),
    }
    (out / MANIFEST).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest


def verify(out: Path) -> dict[str, object]:
    manifest = json.loads((out / MANIFEST).read_text(encoding="utf-8"))
    expected = {"cubrid_python_commit": CUBRID_PYTHON_COMMIT, "cci_commit": CCI_COMMIT}
    for key, value in expected.items():
        if manifest.get(key) != value:
            raise SystemExit(f"{MANIFEST}: {key}={manifest.get(key)!r}, expected {value}")
    if manifest.get("python") != platform.python_version():
        raise SystemExit(
            f"{MANIFEST}: built for Python {manifest.get('python')}, "
            f"running {platform.python_version()}"
        )
    extension = out / str(manifest.get("extension"))
    if manifest.get("extension") != _extension_name() or not extension.is_file():
        raise SystemExit(f"{MANIFEST}: extension {extension.name} missing or mismatched")
    if _sha256(extension) != manifest.get("extension_sha256"):
        raise SystemExit(f"{extension}: SHA-256 does not match {MANIFEST}")
    if not (out / "CUBRIDdb" / "__init__.py").is_file():
        raise SystemExit(f"{out}: CUBRIDdb package missing")
    return dict(manifest)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True, help="importable output root")
    parser.add_argument("--work", type=Path, help="scratch directory (default: <out>.work)")
    parser.add_argument("--verify", action="store_true", help="only verify a cached build")
    args = parser.parse_args()
    out = args.out.resolve()
    if args.verify:
        manifest = verify(out)
    else:
        work = (args.work or out.with_name(out.name + ".work")).resolve()
        build(out, work)
        manifest = verify(out)
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
