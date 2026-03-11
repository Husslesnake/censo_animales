# Censo de Animales

Base de datos para la gestión del censo de animales.

## Estructura de ramas

| Rama              | Propósito                                      |
|-------------------|------------------------------------------------|
| `main`            | Versión estable y productiva                   |
| `develop`         | Integración de cambios en desarrollo           |
| `feature/*`       | Nuevas funcionalidades                         |
| `hotfix/*`        | Correcciones urgentes sobre `main`             |
| `release/*`       | Preparación de versiones para producción       |

## Flujo de trabajo

`
feature/nombre  â†’  develop  â†’  release/v1.x  â†’  main
                                                  â†‘
                               hotfix/fix  â”€â”€â”€â”€â”€â”€â”€â”˜
`

## Convención de commits

- `feat:`     nueva funcionalidad
- `fix:`      corrección de error
- `data:`     actualización de datos
- `docs:`     documentación
- `refactor:` reestructuración sin cambio funcional
