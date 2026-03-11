# ============================================================
#  INICIALIZAR REPOSITORIO - CENSO ANIMALES
#  Ejecutar en la carpeta donde esté el archivo .mdb
#  Requiere Git para Windows instalado
# ============================================================

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "=====================================================" -ForegroundColor Cyan
Write-Host "  Inicializando repositorio Git - Censo Animales" -ForegroundColor Cyan
Write-Host "=====================================================" -ForegroundColor Cyan
Write-Host ""

# Verificar que Git está instalado
try {
    git --version | Out-Null
} catch {
    Write-Host "ERROR: Git no está instalado o no está en el PATH." -ForegroundColor Red
    Write-Host "Descárguelo en: https://git-scm.com/download/win" -ForegroundColor Yellow
    exit 1
}

# Verificar que existe el archivo .mdb
$mdbFile = Get-ChildItem -Filter "*.mdb" | Select-Object -First 1
if (-not $mdbFile) {
    Write-Host "AVISO: No se encontró ningún archivo .mdb en esta carpeta." -ForegroundColor Yellow
    Write-Host "Asegúrese de ejecutar este script en la misma carpeta que el proyecto." -ForegroundColor Yellow
}

# ── Configuración del repositorio ──────────────────────────

git init
git config user.email "usuario@proyecto.com"
git config user.name "Usuario"

Write-Host "Repositorio inicializado." -ForegroundColor Green

# ── .gitignore ─────────────────────────────────────────────

@"
# Bloqueos de Access
*.ldb
*.laccdb
*_backup.mdb
~*.mdb

# Sistema operativo
Thumbs.db
desktop.ini
.DS_Store

# Editores
*.tmp
*.bak
"@ | Out-File -FilePath ".gitignore" -Encoding utf8

# ── README.md ──────────────────────────────────────────────

@"
# Censo de Animales

Base de datos para la gestión del censo de animales.

## Estructura de ramas

| Rama              | Propósito                                      |
|-------------------|------------------------------------------------|
| ``main``          | Versión estable y productiva                   |
| ``develop``       | Integración de cambios en desarrollo           |
| ``feature/*``     | Nuevas funcionalidades                         |
| ``hotfix/*``      | Correcciones urgentes sobre ``main``           |
| ``release/*``     | Preparación de versiones para producción       |

## Flujo de trabajo

```
feature/nombre  →  develop  →  release/v1.x  →  main
                                                  ↑
                               hotfix/fix  ───────┘
```

## Convención de commits

- ``feat:``     nueva funcionalidad
- ``fix:``      corrección de error
- ``data:``     actualización de datos
- ``docs:``     documentación
- ``refactor:`` reestructuración sin cambio funcional
"@ | Out-File -FilePath "README.md" -Encoding utf8

# ── CHANGELOG.md ───────────────────────────────────────────

$fecha = Get-Date -Format "yyyy-MM-dd"
@"
# Changelog

## [1.0.0] - $fecha

### Añadido
- Importación inicial de la base de datos CENSO_ANIMALES
- Configuración del repositorio con estructura Git Flow
"@ | Out-File -FilePath "CHANGELOG.md" -Encoding utf8

# ── Primer commit en main ──────────────────────────────────

git add .
git commit -m "feat: importacion inicial del proyecto CENSO_ANIMALES v1.0.0"
git branch -M main
git tag -a v1.0.0 -m "Version inicial del proyecto"

Write-Host "Rama 'main' creada con commit inicial." -ForegroundColor Green

# ── Rama develop ───────────────────────────────────────────

git checkout -b develop
git commit --allow-empty -m "chore: inicializar rama develop"

Write-Host "Rama 'develop' creada." -ForegroundColor Green

# ── Rama feature de ejemplo ────────────────────────────────

git checkout -b feature/mejora-estructura develop
git commit --allow-empty -m "feat: rama de ejemplo para nuevas funcionalidades"
git checkout develop

Write-Host "Rama 'feature/mejora-estructura' creada como ejemplo." -ForegroundColor Green

# ── Resumen final ──────────────────────────────────────────

Write-Host ""
Write-Host "=====================================================" -ForegroundColor Cyan
Write-Host "  Repositorio listo. Estructura de ramas:" -ForegroundColor Cyan
Write-Host "=====================================================" -ForegroundColor Cyan
git log --oneline --all --graph
Write-Host ""
Write-Host "Ramas disponibles:" -ForegroundColor White
git branch
Write-Host ""
Write-Host "Para vincular con GitHub o GitLab, ejecute:" -ForegroundColor Yellow
Write-Host '  git remote add origin https://github.com/usuario/censo_animales.git' -ForegroundColor Gray
Write-Host '  git push -u origin --all' -ForegroundColor Gray
Write-Host '  git push origin --tags' -ForegroundColor Gray
Write-Host ""
Write-Host "Proceso completado con exito." -ForegroundColor Green
