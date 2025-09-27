# photos_vs_checksums.ps1  — run from repo root

$ErrorActionPreference = "Stop"

$exts = @('.jpg', '.jpeg', '.png', '.webp')
$bagsPath = "data\02_Models"
$checksumsPath = "data\checksums"

# collect any mismatches so the linter sees the value used
$mismatches = New-Object System.Collections.Generic.List[string]

Get-ChildItem -Directory -Path $bagsPath | ForEach-Object {
  $bag = $_.Name
  $photosDir = Join-Path $_.FullName "photos"
  if (-not (Test-Path $photosDir)) { return }

  # count photos by extension
  $photos = (Get-ChildItem -File -Path $photosDir |
             Where-Object { $exts -contains $_.Extension.ToLower() }).Count

  # count lines in checksum manifest
  $shaFile = Join-Path $checksumsPath ($bag + ".sha256")
  if (Test-Path $shaFile) {
    $lines = (Get-Content -LiteralPath $shaFile | Measure-Object -Line).Lines
  } else {
    $lines = 0
  }

  "{0}: photos={1}, manifest_lines={2}" -f $bag, $photos, $lines | Write-Host

  if ($photos -ne $lines) {
    $mismatches.Add("$bag (photos=$photos, checksums=$lines)") | Out-Null
  }
}

if ($mismatches.Count -gt 0) {
  Write-Host "✗ mismatch detected (photos != manifest lines)" -ForegroundColor Red
  $mismatches | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
  exit 2
} else {
  Write-Host "✓ checksums match photo counts for all bags" -ForegroundColor Green
  exit 0
}
