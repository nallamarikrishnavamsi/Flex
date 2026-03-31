$env:PATH = "C:\msys64\ucrt64\bin;$env:PATH"

Write-Output "--- Starting 10M Benchmark (25k Batch) ---"
$server2 = Start-Process -FilePath ".\flexql-server.exe" -ArgumentList "--clean" -PassThru -NoNewWindow
Start-Sleep -Seconds 2
.\benchmark_flexql.exe 10000000 | Out-File bench_10m_25k.txt -Encoding UTF8
Stop-Process -Id $server2.Id -Force

Write-Output "Done"
