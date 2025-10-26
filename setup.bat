@echo off
echo Starting Mental Health Sentiment Dashboard...
cd /d "C:\Users\Suryanarayan\Desktop\Live_Chat_Sentiment_Analysis_KAFKA"
python app.py
pause

echo Step 1: Checking Docker installation...
docker --version
if %errorlevel% neq 0 (
    echo ERROR: Docker is not installed. Please install Docker Desktop first.
    echo Download from: https://www.docker.com/products/docker-desktop/
    pause
    exit /b 1
)

echo Step 2: Checking Docker Compose...
docker-compose --version
if %errorlevel% neq 0 (
    echo ERROR: Docker Compose is not available.
    pause
    exit /b 1
)

echo Step 3: Installing Python dependencies...
pip install -r requirements.txt

echo Step 4: Downloading TextBlob corpora...
python -m textblob.download_corpora

echo.
echo ✅ Setup completed successfully!
echo.
echo Next steps:
echo 1. Make sure Docker Desktop is running
echo 2. Run: python app.py
echo 3. Open: http://localhost:5000
echo.
pause