# Run from the frozen repository root with an empty recording venv.
# Root supplies VIRTUAL_ENV and DEMO_* including the exact owned table;
# the visible install downloads the pinned release from PyPI.
Require pip
Require python
Output .demo-artifacts/demo.mp4
Set Shell bash
Set FontSize 18
Set Width 1000
Set Height 650
Set Theme "Catppuccin Mocha"
Set Framerate 30
Set PlaybackSpeed 1
Set TypingSpeed 45ms
Set WaitTimeout 20s
Env PIP_DISABLE_PIP_VERSION_CHECK "1"
Env PIP_PROGRESS_BAR "off"
Env PIP_INDEX_URL "https://pypi.org/simple"
Env PIP_NO_CACHE_DIR "1"

# Restore the supplied venv after a shell login profile might reset PATH.
Hide
Type `test -n "$VIRTUAL_ENV" && test -x "$VIRTUAL_ENV/bin/python" || exit 1; export PATH="$VIRTUAL_ENV/bin:$PATH"; clear`
Enter
Sleep 200ms
Show

Type "pip install pycubrid==1.17.0"
Enter
Wait+Screen /Successfully installed pycubrid/
Sleep 5s
Type "python demos/record_demo.py extended"
Enter
Wait+Screen /DEMO_EXTENDED_OK\r?\n/
# Hold actual CRUD/commit/async results for reading; never synthesize their output.
Sleep 24s
