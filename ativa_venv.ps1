# ============================
# Script: ativa_venv.ps1
# Objetivo: criar/ativar venv, instalar dependências e contornar bloqueio de execução
# ============================
# powershell -ExecutionPolicy Bypass -File .\ativa_venv.ps1
# Exibir cabeçalho
Write-Host "==== Ativando ambiente virtual Python ====" -ForegroundColor Cyan

# Guardar política original de execução
$originalPolicy = Get-ExecutionPolicy

# Permitir execução local temporariamente
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force

# Caminho base do projeto
$projectPath = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $projectPath

# Criar venv se não existir
if (-Not (Test-Path "$projectPath\venv")) {
    Write-Host "Criando ambiente virtual em: $projectPath\venv" -ForegroundColor Yellow
    python -m venv venv
}

# Ativar a venv
Write-Host "Ativando ambiente virtual..." -ForegroundColor Yellow
& "$projectPath\venv\Scripts\Activate.ps1"

# Atualizar pip
Write-Host "Atualizando pip..." -ForegroundColor Yellow
python -m pip install --upgrade pip

# Instalar dependências
if (Test-Path "$projectPath\requirements.txt") {
    Write-Host "Instalando dependências de requirements.txt..." -ForegroundColor Yellow
    pip install -r "$projectPath\requirements.txt"
} else {
    Write-Host "⚠️  Nenhum arquivo requirements.txt encontrado." -ForegroundColor Red
}

# Mensagem final
Write-Host "Ambiente virtual ativado e dependências instaladas!" -ForegroundColor Green

# Restaurar política original
Set-ExecutionPolicy -Scope Process -ExecutionPolicy $originalPolicy -Force
