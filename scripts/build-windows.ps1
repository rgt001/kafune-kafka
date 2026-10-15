param([ValidateSet('check','test','build','clippy','fmt')][string]$Command='build',[switch]$Release)
$ErrorActionPreference='Stop'
$projectRoot = Split-Path $PSScriptRoot -Parent
$cargoBin = Join-Path $env:USERPROFILE '.cargo\bin'
$localCmake = Join-Path $projectRoot '.tools\python\cmake\data\bin'
$localNasm = Join-Path $projectRoot '.tools\nasm\nasm-2.16.03'
$localPerl = Join-Path $projectRoot '.tools\strawberry\perl\bin'
$env:PATH = $cargoBin + ';' + $localCmake + ';' + $localNasm + ';' + $localPerl + ';C:\Program Files\Git\usr\bin;' + $env:PATH
$env:CARGO_BUILD_JOBS='8'
$cargoArgs = @($Command,'--manifest-path',(Join-Path $projectRoot 'Cargo.toml'))
if ($Release -and $Command -eq 'build') { $cargoArgs += '--release' }
& cargo @cargoArgs
if ($LASTEXITCODE -ne 0) { throw ('Cargo '+$Command+' falhou') }
