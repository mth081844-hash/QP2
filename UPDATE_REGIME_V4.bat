@echo off
chcp 65001 > nul
echo ============================================================
echo   QP2 REGIME v4 업데이트
echo   (3레짐: Bull / Neutral / Bear + 거시필터)
echo ============================================================
echo.

cd /d C:\QP2
call .venv\Scripts\activate.bat

python scripts\update_regime_v4.py

echo.
pause
