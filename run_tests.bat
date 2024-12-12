@echo off
echo Running tests...

REM Répertoire des classes compilées
set BIN_DIR=bin

REM Tester les classes une par une
java -ea -cp %BIN_DIR% miniSGBDR.BufferManagerTests
java -ea -cp %BIN_DIR% miniSGBDR.DBConfigTests
java -ea -cp %BIN_DIR% miniSGBDR.DiskManagerTests
java -ea -cp %BIN_DIR% miniSGBDR.RecordTests
java -ea -cp %BIN_DIR% miniSGBDR.RelationTests
java -ea -cp %BIN_DIR% miniSGBDR.RelationTests

REM Vérification des tests
if %ERRORLEVEL% NEQ 0 (
    echo Some tests failed.
    exit /b 1
)

echo All tests ran successfully.
