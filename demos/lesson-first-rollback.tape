# Screenshot for the "Commit and Roll Back" lesson (#1307). Run from an empty folder that holds
# the lesson's files, with the lesson virtual environment active and the
# disposable local lab from the Quick Start running. See demos/README.md.
Require python
Output .demo-artifacts/lesson-first-rollback.gif
Set Shell bash
Set FontSize 16
Set Width 840
Set Height 160
Set Padding 16
Set Theme "Catppuccin Mocha"
Set TypingSpeed 40ms
Set WaitTimeout 60s

# Neutral prompt and the lesson venv first on PATH, hidden from the capture.
Hide
Type `test -n "$VIRTUAL_ENV" && test -x "$VIRTUAL_ENV/bin/python" || exit 1; export PATH="$VIRTUAL_ENV/bin:$PATH" PS1='$ '; clear`
Enter
Sleep 500ms
Show

Type "python first_rollback.py"
Enter
Wait+Screen /After rollback: \[\(1/
Sleep 500ms
Screenshot .demo-artifacts/lesson-first-rollback.png
Sleep 200ms
