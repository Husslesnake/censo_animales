# Censo de Animales

Base de datos para la gestiÃ³n del censo de animales.

## Estructura de ramas

| Rama              | PropÃ³sito                                      |
|-------------------|------------------------------------------------|
| `main`          | VersiÃ³n estable y productiva                   |
| `develop`       | IntegraciÃ³n de cambios en desarrollo           |
| `feature/*`     | Nuevas funcionalidades                         |
| `hotfix/*`      | Correcciones urgentes sobre `main`           |
| `release/*`     | PreparaciÃ³n de versiones para producciÃ³n       |

## Flujo de trabajo

`
feature/nombre  â†’  develop  â†’  release/v1.x  â†’  main
                                                  â†‘
                               hotfix/fix  â”€â”€â”€â”€â”€â”€â”€â”˜
`

## ConvenciÃ³n de commits

- `feat:`     nueva funcionalidad
- `fix:`      correcciÃ³n de error
- `data:`     actualizaciÃ³n de datos
- `docs:`     documentaciÃ³n
- `refactor:` reestructuraciÃ³n sin cambio funcional
