@echo off

set SRC_DIR=src
set TEST_DIR=test
set SUBMISSION_ZIP=submission.zip
set WAIT_TIME=5
set JAVA_HOME=C:\Program Files\Java\jdk-21.0.2

echo Creating test directory...
if not exist %TEST_DIR% mkdir %TEST_DIR%

echo Copying files to test directory...
copy %SRC_DIR%\Controller.java %TEST_DIR%
copy %SRC_DIR%\Dstore.java %TEST_DIR%
copy %SRC_DIR%\Protocol.java %TEST_DIR%
copy %SRC_DIR%\my_policy.policy %TEST_DIR%
copy %SRC_DIR%\ClientMain.java %TEST_DIR%
copy %SRC_DIR%\client.jar %TEST_DIR%

echo Creating submission zip file...
"%JAVA_HOME%\bin\jar" cMf %TEST_DIR%\%SUBMISSION_ZIP% %TEST_DIR%\Controller.java %TEST_DIR%\Dstore.java

echo Compiling Java files...
javac -cp %TEST_DIR%\client.jar %TEST_DIR%\*.java

echo Starting Controller...
start "Controller" java -Djava.security.manager -Djava.security.policy=%TEST_DIR%\my_policy.policy -cp %TEST_DIR% Controller 12345 3 1000 30

echo Starting Dstores...
start "Dstore1" java -Djava.security.manager -Djava.security.policy=%TEST_DIR%\my_policy.policy -cp %TEST_DIR% Dstore 12346 12345 1000 %TEST_DIR%\dstore1_files
start "Dstore2" java -Djava.security.manager -Djava.security.policy=%TEST_DIR%\my_policy.policy -cp %TEST_DIR% Dstore 12347 12345 1000 %TEST_DIR%\dstore2_files
start "Dstore3" java -Djava.security.manager -Djava.security.policy=%TEST_DIR%\my_policy.policy -cp %TEST_DIR% Dstore 12348 12345 1000 %TEST_DIR%\dstore3_files

echo Waiting for %WAIT_TIME% seconds...
timeout /t %WAIT_TIME% /nobreak

echo Running tests...
java -cp %TEST_DIR%;%TEST_DIR%\client.jar ClientMain 12345 1000

echo Tests completed.
pause