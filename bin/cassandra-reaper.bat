
@echo off

pushd %~dp0..
IF NOT DEFINED REAPER_HOME set REAPER_HOME=%CD%
popd

IF NOT DEFINED REAPER_JAR (
    for %%i in ("%REAPER_HOME%\target\cassandra-reaper-*.jar") do set REAPER_JAR=%%i
)

IF NOT DEFINED REAPER_CONF set REAPER_CONF=%REAPER_HOME%\resource\cassandra-reaper.yaml

java -jar %REAPER_JAR% server %REAPER_CONF%
