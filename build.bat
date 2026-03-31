@echo off
set PATH=C:\msys64\ucrt64\bin;%PATH%
echo === Building Server === > build_log.txt 2>&1
g++ -std=c++20 -O3 -march=native -flto -DNDEBUG -Iflexql/include flexql/src/server/server_main.cpp flexql/src/server/server.cpp flexql/src/network/socket_utils.cpp flexql/src/parser/sql_parser.cpp flexql/src/storage/database_engine.cpp flexql/src/storage/wal_writer.cpp -o flexql-server.exe -lws2_32 >> build_log.txt 2>&1
echo Server exit: %ERRORLEVEL% >> build_log.txt 2>&1
echo === Building Benchmark === >> build_log.txt 2>&1
g++ -std=c++20 -O3 -march=native -flto -DNDEBUG -Iflexql/include benchmark_flexql.cpp flexql/src/client/flexql_api.cpp flexql/src/network/socket_utils.cpp flexql/src/parser/sql_parser.cpp flexql/src/storage/database_engine.cpp flexql/src/storage/wal_writer.cpp -o benchmark_flexql.exe -lws2_32 >> build_log.txt 2>&1
echo Bench exit: %ERRORLEVEL% >> build_log.txt 2>&1
echo === Building Multi-Client Bench === >> build_log.txt 2>&1
g++ -std=c++20 -O3 -march=native -flto -DNDEBUG -Iflexql/include multiclient_bench.cpp flexql/src/client/flexql_api.cpp flexql/src/network/socket_utils.cpp flexql/src/parser/sql_parser.cpp flexql/src/storage/database_engine.cpp flexql/src/storage/wal_writer.cpp -o multiclient_bench.exe -lws2_32 >> build_log.txt 2>&1
echo MCBench exit: %ERRORLEVEL% >> build_log.txt 2>&1
