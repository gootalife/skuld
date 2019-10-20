@echo off
if "%1"=="" (
    echo arg error: Input a target project name.
) else (
    perl extracting.pl data\projects\%1\logs\commits data\projects\%1\logs\preprocessed
)
