@echo off
echo ============================================================
echo   Building RetailPulse_DP900.exe (Windows Standalone EXE)
echo ============================================================
python -m pip install --upgrade pip
python -m pip install pyinstaller
python retailpulse_app.py --self-test
pyinstaller --onefile --console --name RetailPulse_DP900 retailpulse_app.py
echo.
echo Build complete! Find your executable at: dist\RetailPulse_DP900.exe
pause
