@echo off
echo Restarting MapYourWay Dashboard with MapTiler Basemaps...
echo.

REM Kill any existing Streamlit processes
taskkill /F /IM streamlit.exe 2>nul
timeout /t 2 /nobreak >nul

REM Set environment variables
set PYTHONPATH=c:\Users\archa\OneDrive\Desktop\BDA\src

REM Start dashboard
cd /d c:\Users\archa\OneDrive\Desktop\BDA
echo Starting dashboard at http://localhost:8501
echo Press Ctrl+C to stop
streamlit run src\mapyourway\dashboard\app.py --server.port 8501
