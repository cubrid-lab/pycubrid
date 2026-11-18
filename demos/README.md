# Reproducible terminal recordings

The README GIF and Quick Start MP4 show real execution of `record_demo.py`,
not illustrative output. The script asserts
its results and prints its final success marker only after resource cleanup.
These examples verify their recorded scenarios, not full driver compatibility.

## Inputs and containment

Use a dedicated disposable CUBRID database/broker that you own. Do not point
the extended demo at a production or shared database. Its uniquely named
fixture must not exist beforehand; it is dropped after the run. Use only
nonsecret inputs for a terminal recording and never display a password/DSN.
The committed recording's exact source/build/engine evidence is in
`recording-provenance.json`.

## Published release and recording environment

The recordings install a **published PyPI release**, the version pinned in the
tapes' `pip install pycubrid==X.Y.Z` line. When you re-record for a newer
release, update that pin in both tapes and in `recording-provenance.json`.
Start with a fresh, empty venv for each recording:

```bash
python -m venv .demo-artifacts/venv
source .demo-artifacts/venv/bin/activate
```

The tapes set `PIP_INDEX_URL=https://pypi.org/simple` and
`PIP_NO_CACHE_DIR=1`, so the visible install downloads the wheel from PyPI
rather than from a mirror, a local wheel directory or pip's cache. After
recording, confirm that the installed distribution and version, and
`pycubrid.__file__`, belong to the recording venv and not to the repository
checkout. Run that check from outside the checkout: `python -c` started from the
repository root imports the source tree instead.

Set the script's endpoint, expected actual engine build and unique fixture
inputs as documented in `record_demo.py`. Run both modes without VHS first;
capture their zero exit codes and output. An unexpected value, existing fixture
or failed cleanup must fail rather than print the success marker.

## Render and promote

Use VHS v0.12.1 with its existing ttyd/Chrome/ffmpeg requirements. Version
0.12.0 cancels the encoder context and can exit successfully without creating
media; [the upstream fix](https://github.com/charmbracelet/vhs/pull/788)
is included in 0.12.1. Verify output files, not only the tool's exit code. From the
repository root, with the isolated environment still active:

```bash
vhs validate demos/pycubrid-demo.tape demos/pycubrid-demo-video.tape
timeout 180s vhs demos/pycubrid-demo.tape
timeout 180s vhs demos/pycubrid-demo-video.tape
test -s .demo-artifacts/demo.gif
test -s .demo-artifacts/demo.mp4
```

Both tapes write only ignored `.demo-artifacts/` staging paths. Verify the
recording shell actually uses the recording venv and script before
rendering; shell startup configuration must not substitute another `python`
or `pip`. A VHS marker wait is not a substitute for the separate real exit
and cleanup checks.

Inspect the entire GIF/video as well as sampled frames for legibility and
absence of errors/secrets. Use `ffprobe` for dimensions/duration/codec: GIF
8–12 seconds at 800–1000 pixels wide; MP4 30–60 seconds, H.264. Only after
review promote to `docs/demo.gif` and
`docs/assets/videos/pycubrid-demo.mp4`. Keep editable sources, generated media,
provenance and documentation together. No VHS publish, external upload, package
version change or release action is part of this workflow.
