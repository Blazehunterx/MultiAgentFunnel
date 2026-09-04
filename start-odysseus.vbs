Set WshShell = CreateObject("WScript.Shell")
WshShell.CurrentDirectory = "C:\Users\marvi\odysseus"
WshShell.Run """C:\Users\marvi\odysseus\venv\Scripts\chroma.exe"" run --host 127.0.0.1 --port 8100", 0, False
WScript.Sleep 8000
WshShell.Run """C:\Users\marvi\odysseus\venv\Scripts\python.exe"" -m uvicorn app:app --host 127.0.0.1 --port 7000", 0, False
