# pycubrid — Pure Python CUBRID driver demo
Output ../docs/demo.gif
Set FontSize 16
Set Width 1200
Set Height 600
Set Theme "Catppuccin Mocha"
Set Shell bash

# Show the package install — one command, no C compiler
Type@50ms "pip install pycubrid"
Enter
Sleep 3s

# Python REPL
Type@50ms "python3"
Enter
Sleep 1s

# Import and connect
Type@30ms "import pycubrid"
Enter
Sleep 500ms

Type@30ms 'conn = pycubrid.connect(host="localhost", port=33000, database="testdb", user="dba")'
Enter
Sleep 1s

Type@30ms 'print(f"Connected: {conn.get_server_version()}")'
Enter
Sleep 1s

# Query
Type@30ms "cur = conn.cursor()"
Enter
Sleep 300ms

Type@30ms 'cur.execute("SELECT 1 + 1 AS result")'
Enter
Sleep 300ms

Type@30ms "print(cur.fetchone())"
Enter
Sleep 1s

# Show zero dependencies
Type@30ms 'print(f"Dependencies: 0 — pure Python")'
Enter
Sleep 1s

# Clean exit
Type@30ms "cur.close(); conn.close()"
Enter
Sleep 500ms

Ctrl+D
