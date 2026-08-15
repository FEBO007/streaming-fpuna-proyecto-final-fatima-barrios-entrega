$ErrorActionPreference = "Stop"

& .\.venv\Scripts\python.exe .\scripts\verify_project.py
exit $LASTEXITCODE
