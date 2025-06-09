<#  update-menu.ps1
    Copy the live PAD file into the Git repo,
    then commit & push if it changed. #>

# --- EDIT THESE TWO LINES ONLY ------------------------------------------
$SourceFile = 'C:\Users\Austin.S\OneDrive - Ceres Naturals\Ceres Public Share - METRC API Depot\filtered_labresultsCM.json'
$RepoDir    = 'C:\menu-git'
# ------------------------------------------------------------------------

$TargetFile = Join-Path $RepoDir 'menu.json'

# Pull latest repo (quietly)
git -C $RepoDir pull --quiet

# Copy the newest PAD export over the old one
Copy-Item -Path $SourceFile -Destination $TargetFile -Force

# If there’s no change, stop here
$status = git -C $RepoDir status --porcelain
if (-not $status) {
    Write-Host 'No changes – nothing to commit.'
    exit
}

# Commit & push
$stamp = Get-Date -Format 'yyyy-MM-dd HH:mm'
git -C $RepoDir add menu.json
git -C $RepoDir commit -m "menu auto-update $stamp"
git -C $RepoDir push origin main

Write-Host "✅ Menu pushed at $stamp"