@ ECHO OFF

setlocal ENABLEDELAYEDEXPANSION
set word=/
set str=%~dp0
set str=%str:\=!word!%

@ echo setwd('%str%') > R\working_directory.R

TASKKILL /F /IM EXCEL.exe

CALL "C:\Program Files\R\R-3.4.2\bin\x64\RScript.exe" "R\working_directory.R"

CALL "C:\Program Files\R\R-3.4.2\bin\x64\RScript.exe" "R\etl.R" >> R\R_log.txt

PAUSE

