# Run from the frozen repository root; outputs remain staging-only.
# Root supplies an empty VIRTUAL_ENV and the nonsecret DEMO_* endpoint.
# The visible install downloads the pinned release from PyPI.
Require pip
Require python
Output .demo-artifacts/demo.gif
Set Shell bash
Set FontSize 16
Set Width 900
Set Height 520
Set Theme "Catppuccin Mocha"
Set Framerate 12
Set PlaybackSpeed 1
Set TypingSpeed 20ms
Set WaitTimeout 10s
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
Sleep 300ms
Type "python demos/record_demo.py short"
Enter
# A standalone script marker, not text from a typed command or traceback quote.
Wait+Screen /DEMO_SHORT_OK\r?\n/
Sleep 6s
