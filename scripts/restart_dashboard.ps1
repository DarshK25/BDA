Write-Host "Restarting MapYourWay Dashboard with MapTiler Basemaps..." -ForegroundColor Cyan
Write-Host ""

# Stop any existing Streamlit processes
Write-Host "Stopping existing Streamlit processes..." -ForegroundColor Yellow
Get-Process | Where-Object {$_.ProcessName -eq "streamlit"} | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# Set environment
$env:PYTHONPATH = "c:\Users\archa\OneDrive\Desktop\BDA\src"

# Change to project directory
Set-Location "c:\Users\archa\OneDrive\Desktop\BDA"

Write-Host ""
Write-Host "Starting dashboard at http://localhost:8501" -ForegroundColor Green
Write-Host "Press Ctrl+C to stop" -ForegroundColor Yellow
Write-Host ""

# Start dashboard (loads .env automatically)
streamlit run src\mapyourway\dashboard\app.py --server.port 8501
