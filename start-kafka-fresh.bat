@echo off
cls
echo ===================================================
echo [1/3] Wiping existing KRaft logs...
echo ===================================================
:: Safely delete the log directory if it exists, then recreate it empty
if exist "C:\kafka\kraft-combined-logs" (
    rmdir /s /q "C:\kafka\kraft-combined-logs"
)
mkdir "C:\kafka\kraft-combined-logs"

echo ===================================================
echo [2/3] Generating Cluster ID and Formatting Storage...
echo ===================================================
:: Generate a fresh unique cluster ID
for /f "tokens=*" %%i in ('kafka-storage random-uuid') do set CLUSTER_ID=%%i
echo Generated Cluster ID: %CLUSTER_ID%

:: Format using the corrected argument syntax parameters
call kafka-storage format --cluster-id %CLUSTER_ID% --config C:\kafka\config\server.properties --standalone

echo ===================================================
echo [3/3] Launching Kafka Server in KRaft Mode...
echo ===================================================
:: Spin up the broker using the correct KRaft configuration profile
call kafka-server-start C:\kafka\config\server.properties

pause
