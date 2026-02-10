@echo off
poetry --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Poetry not found
    exit /b 1
)
call poetry install --no-root
call poetry run python -m app.main %*
