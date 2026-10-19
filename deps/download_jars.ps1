$ErrorActionPreference = "Stop"

$deps = $PSScriptRoot
$manifest = Join-Path $deps "jars.sha256"
$destination = Join-Path $deps "jars"

if (-not (Test-Path $manifest)) {
    throw "Missing jars.sha256"
}

New-Item -ItemType Directory -Force $destination | Out-Null

foreach ($line in Get-Content $manifest) {
    if ($line -notmatch '^([a-fA-F0-9]{64})\s+\*(.+)$') {
        continue
    }

    $expected = $matches[1]
    $name = $matches[2]

    if ($name -eq "hadoop-bare-naked-local-fs-0.1.0.jar") {
        $url = "https://repo.maven.apache.org/maven2/com/globalmentor/hadoop-bare-naked-local-fs/0.1.0/hadoop-bare-naked-local-fs-0.1.0.jar"
    }
    elseif ($name -match '^(.+?)_(.+)-([0-9][0-9A-Za-z._-]*)\.jar$') {
        $group = $matches[1].Replace(".", "/")
        $artifact = $matches[2]
        $version = $matches[3]
        $url = "https://repo.maven.apache.org/maven2/$group/$artifact/$version/$artifact-$version.jar"
    }
    else {
        throw "Cannot determine Maven coordinates for $name"
    }

    $target = Join-Path $destination $name

    if (-not (Test-Path $target)) {
        Write-Host "Downloading $name"
        Invoke-WebRequest -Uri $url -OutFile $target -UseBasicParsing
    }

    $actual = (Get-FileHash $target -Algorithm SHA256).Hash

    if ($actual -ine $expected) {
        throw "SHA256 mismatch: $name"
    }

    Write-Host "PASS: $name"
}

Write-Host "All JAR dependencies verified."
