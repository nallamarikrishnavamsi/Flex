$env:PATH = "C:\msys64\ucrt64\bin;$env:PATH"

Write-Host "=== Killing old processes ==="
Stop-Process -Name "flexql-server" -Force -ErrorAction SilentlyContinue
Stop-Process -Name "benchmark_flexql" -Force -ErrorAction SilentlyContinue
Stop-Process -Name "multiclient_bench" -Force -ErrorAction SilentlyContinue
Start-Sleep 3

Write-Host "=== Cleaning WAL ==="
Remove-Item "flexql_data\wal.log" -Force -ErrorAction SilentlyContinue

Write-Host "=== Starting server ==="
$srv = Start-Process -FilePath ".\flexql-server.exe" -PassThru
Start-Sleep 2

Write-Host "=== Running single-client 10M benchmark ==="
& ".\benchmark_flexql.exe" 10000000 2>&1 | Tee-Object -FilePath "bench_single.txt"

Write-Host ""
Write-Host "=== Stopping server, cleaning WAL ==="
Stop-Process -Id $srv.Id -Force -ErrorAction SilentlyContinue
Start-Sleep 3
Remove-Item "flexql_data\wal.log" -Force -ErrorAction SilentlyContinue

Write-Host "=== Starting fresh server ==="
$srv = Start-Process -FilePath ".\flexql-server.exe" -PassThru
Start-Sleep 2

Write-Host "=== Running multi-client 8T benchmark ==="
& ".\multiclient_bench.exe" --threads 8 --rows 1250000 --mode write 2>&1 | Tee-Object -FilePath "bench_multi8.txt"

Write-Host ""
Write-Host "=== Stopping server, cleaning WAL ==="
Stop-Process -Id $srv.Id -Force -ErrorAction SilentlyContinue
Start-Sleep 3
Remove-Item "flexql_data\wal.log" -Force -ErrorAction SilentlyContinue

Write-Host "=== Starting fresh server ==="
$srv = Start-Process -FilePath ".\flexql-server.exe" -PassThru
Start-Sleep 2

Write-Host "=== Running multi-client 4T benchmark ==="
& ".\multiclient_bench.exe" --threads 4 --rows 2500000 --mode write 2>&1 | Tee-Object -FilePath "bench_multi4.txt"

Stop-Process -Id $srv.Id -Force -ErrorAction SilentlyContinue

Write-Host ""
Write-Host "========================================="
Write-Host "ALL BENCHMARKS COMPLETE"
Write-Host "========================================="
