@echo off
chcp 65001 > nul
echo ============================================================
echo   QP2 MACRO 업데이트
echo ============================================================
echo.

cd /d C:\QP2
call .venv\Scripts\activate.bat

python scripts\update_macro.py

echo.
pause