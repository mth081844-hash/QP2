@echo off
title QP2 Data Update
cd /d C:\QP2
call .venv\Scripts\activate.bat
python scripts\update_all.py
pause