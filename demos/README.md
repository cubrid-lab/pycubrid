# Reproducible terminal recordings

The README GIF and Quick Start MP4 show real execution of `record_demo.py`,
not the illustrative output in the older JSON renderer. The script asserts
its results and prints its final success marker only after resource cleanup.
These examples verify their recorded scenarios, not full driver compatibility.

## Inputs and containment

Use a dedicated disposable CUBRID database/broker that you own. Do not point
the extended demo at a production or shared database. Its uniquely named
fixture must not exist beforehand; it is dropped after the run. Use only
nonsecret inputs for a terminal recording and never display a password/DSN.
The committed recording's exact source/build/engine evidence is in
`recording-provenance.json`.

## Source wheel and recording environment

Build one wheel from a clean checkout of the recorded source revision:

```bash
python -m build --wheel --outdir .demo-artifacts/wheels
python -m venv .demo-artifacts/venv
source .demo-artifacts/venv/bin/activate
export PIP_NO_INDEX=1
export PIP_FIND_LINKS="$PWD/.demo-artifacts/wheels"
export PIP_DISABLE_PIP_VERSION_CHECK=1
export PIP_PROGRESS_BAR=off
```

Only the selected local wheel belongs in that directory. The visible
`pip install pycubrid` is therefore a **local source-build installation**, not
a PyPI release check. Start with a fresh venv for each recording. Confirm the
installed distribution/version and `pycubrid.__file__` belong to this venv,
not an editable checkout or another package installation.

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
recording shell actually uses the selected venv/wheel and script before
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
