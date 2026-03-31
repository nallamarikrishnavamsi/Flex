$env:PATH = "C:\msys64\ucrt64\bin;$env:PATH"

Write-Output "--- Starting 10M Benchmark (250k Batch) ---"
$server3 = Start-Process -FilePath ".\flexql-server.exe" -ArgumentList "--clean" -PassThru -NoNewWindow
Start-Sleep -Seconds 2
.\benchmark_flexql.exe 10000000 | Out-File bench_10m_250k_threaded.txt -Encoding UTF8
Stop-Process -Id $server3.Id -Force

Write-Output "Done"
