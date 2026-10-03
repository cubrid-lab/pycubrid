# Run from the frozen repository root with a verified local-wheel recording venv.
# Root supplies VIRTUAL_ENV/PIP_FIND_LINKS and DEMO_* including the exact owned table.
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
Env PIP_NO_INDEX "1"
Env PIP_DISABLE_PIP_VERSION_CHECK "1"

# Restore the supplied venv after a shell login profile might reset PATH.
Hide
Type `test -n "$VIRTUAL_ENV" && test -x "$VIRTUAL_ENV/bin/python" && test -n "$PIP_FIND_LINKS" || exit 1; export PATH="$VIRTUAL_ENV/bin:$PATH"; clear`
Enter
Sleep 200ms
Show

Type "echo 'Local source wheel; no PyPI release'"
Enter
Sleep 3s
Type "pip install pycubrid"
Enter
Wait+Screen /Successfully installed pycubrid/
Sleep 5s
Type "python demos/record_demo.py extended"
Enter
Wait+Screen /DEMO_EXTENDED_OK\r?\n/
# Hold actual CRUD/commit/async results for reading; never synthesize their output.
Sleep 24s
