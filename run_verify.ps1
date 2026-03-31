$env:PATH = "C:\msys64\ucrt64\bin;$env:PATH"

Write-Output "--- Unit Tests ---"
$server1 = Start-Process -FilePath ".\flexql-server.exe" -ArgumentList "--clean" -PassThru -NoNewWindow
Start-Sleep -Seconds 2
.\benchmark_flexql.exe --unit-test | Out-File test_results.txt -Encoding UTF8
Stop-Process -Id $server1.Id -Force
Start-Sleep -Seconds 1

Write-Output "--- 10M Benchmark (25k batch, threaded) ---"
$server2 = Start-Process -FilePath ".\flexql-server.exe" -ArgumentList "--clean" -PassThru -NoNewWindow
Start-Sleep -Seconds 2
.\benchmark_flexql.exe 10000000 | Out-File bench_10m_threaded.txt -Encoding UTF8
Stop-Process -Id $server2.Id -Force

Write-Output "Done"
