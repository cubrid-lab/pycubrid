"""The development-documentation notice on every site page (#960).

``docs/overrides/partials/channel.html`` labels the site as built from the
``main`` branch and names the commit when the build records one. These tests
render the partial with Jinja2, which the ``test`` dependency group installs
(#971), and check that the label never depends on a version string.
"""

from __future__ import annotations

import re
from pathlib import Path
from types import SimpleNamespace

import jinja2
import pytest
import yaml

# A documentation tooling check: it runs in the repo-tooling-tests lane, which
# ci.yml also selects for changes under docs/overrides/.
pytestmark = pytest.mark.repo_tooling

ROOT = Path(__file__).resolve().parent.parent
PARTIAL = ROOT / "docs" / "overrides" / "partials" / "channel.html"
SHA = "4efe1307031b43a300bbb7fd675292e14cedc933"


def _render(revision: str | None, url: str = "quickstart/") -> str:
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(str(PARTIAL.parent.parent)), autoescape=True
    )
    template = env.get_template("partials/channel.html")
    config = SimpleNamespace(extra={"source_revision": revision})
    return " ".join(template.render(config=config, page=SimpleNamespace(url=url)).split())


def test_names_the_commit_when_the_build_records_one() -> None:
    html = _render(SHA)
    assert "Development documentation:" in html
    assert f'href="https://github.com/cubrid-lab/pycubrid/commit/{SHA}"' in html
    assert f"<code>{SHA[:7]}</code>" in html
    assert "recorded no commit" not in html


@pytest.mark.parametrize("revision", ["", "  ", None])
def test_says_no_commit_was_recorded_for_a_local_build(revision: str | None) -> None:
    html = _render(revision)
    assert "(this build recorded no commit)" in html
    assert "/commit/" not in html


def test_korean_pages_get_the_korean_notice() -> None:
    html = _render(SHA, url="ko/quickstart/")
    assert "개발 문서:" in html
    assert f"<code>{SHA[:7]}</code>" in html
    assert "Development documentation" not in html
    assert "커밋이 기록되지 않음" in _render("", url="ko/")


def test_points_to_the_release_list_and_never_claims_stable() -> None:
    for html in (_render(SHA), _render(""), _render(SHA, url="ko/")):
        assert 'href="https://github.com/cubrid-lab/pycubrid/releases"' in html
        assert not re.search(r"\b(stable|latest)\b", html, re.IGNORECASE)


def test_the_label_does_not_come_from_a_version_or_tag() -> None:
    # Only the template logic matters, not the prose: no Jinja expression may
    # read a version, a tag or a release name.
    source = PARTIAL.read_text(encoding="utf-8")
    logic = " ".join(re.findall(r"\{[{%](.*?)[%}]\}", source, re.DOTALL)).lower()
    assert "source_revision" in logic
    for word in ("version", "tag", "release", "manifest"):
        assert word not in logic


def test_mkdocs_reads_the_revision_from_github_sha() -> None:
    class Loader(yaml.SafeLoader):
        pass

    Loader.add_multi_constructor("!", lambda loader, suffix, node: (suffix, node))
    Loader.add_multi_constructor("tag:yaml.org,2002:python/", lambda *args: None)
    config = yaml.load((ROOT / "mkdocs.yml").read_text(encoding="utf-8"), Loader=Loader)
    tag, node = config["extra"]["source_revision"]
    assert tag == "ENV"
    assert [item.value for item in node.value] == ["GITHUB_SHA", ""]
    assert config["theme"]["custom_dir"] == "docs/overrides"
