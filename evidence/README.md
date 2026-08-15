# Evidencias de verificación

Este directorio reúne los resultados técnicos utilizados para comprobar la calidad del código, el recorrido end-to-end y el comportamiento de distribución por particiones.

| Archivo | Contenido |
|---|---|
| `01-verificacion-proyecto-final.log` | Resultado de pruebas automatizadas, análisis estático, formato y validación de Docker Compose. |
| `21-alertas-directrunner-final-20260809-190216.log` | Registro normalizado de los tres mensajes de alerta observados en Kafka. |
| `22-resumen-validacion-final.md` | Informe consolidado de consumo, offsets y salidas de la validación end-to-end. |
| `23-analisis-skew-offline.log` | Comparación reproducible entre una distribución balanceada y un escenario con hot key. |
| `SHA256SUMS.txt` | Sumas SHA-256 para comprobar la integridad de los archivos anteriores y de este inventario. |

## Alcance

La validación end-to-end utiliza un entorno local con un broker Kafka, Apache Beam DirectRunner y una entrada acotada de ocho registros. Confirma la integración entre productor, Kafka, pipeline, tópicos de salida y materializador; no evalúa rendimiento, alta disponibilidad ni recuperación distribuida ante fallos.

La política temporal completa —incluidos panes `ON_TIME`, `LATE` y descarte fuera del horizonte permitido— se verifica de forma determinista con TestStream dentro de la suite automatizada.

## Reproducción e integridad

El procedimiento completo se encuentra en [`docs/prueba-desde-cero.md`](../docs/prueba-desde-cero.md). Las sumas de integridad y las verificaciones automatizadas pueden ejecutarse con:

```text
python scripts/verify_project.py
```
