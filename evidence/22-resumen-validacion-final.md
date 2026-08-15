# Informe de validación end-to-end

**Fecha:** 9 de agosto de 2026

**Recorrido:** productor -> Kafka -> Apache Beam DirectRunner -> Kafka

**Grupo de consumo:** `velocity-beam-smoke-v12`

## Resultado del proceso

```text
PIPELINE_EXIT_CODE=0
```

El pipeline finalizó correctamente después de procesar los ocho registros previstos.

## Offsets del grupo al finalizar

| Partición | `CURRENT-OFFSET` | `LOG-END-OFFSET` | `LAG` |
|---:|---:|---:|---:|
| 0 | 0 | 0 | 0 |
| 1 | 2 | 2 | 0 |
| 2 | 14 | 14 | 0 |

El grupo consumió ocho registros desde las posiciones preparadas y terminó con `LAG=0`.

## Tópicos de salida

```text
risk.alerts.velocity:0:3
bank.transactions.invalid:0:0
```

Se publicaron tres alertas y ningún registro inválido.

## Alertas observadas

| Offset | Ventana UTC | Conteo | Total PYG | Condición | Pane | Revisión |
|---:|---|---:|---:|---|---|---:|
| 0 | 19:59:50-20:00:50 | 5 | 10.500.000 | conteo y monto | `ON_TIME` | 0 |
| 1 | 19:59:40-20:00:40 | 4 | 8.000.000 | conteo | `ON_TIME` | 0 |
| 2 | 20:00:00-20:01:00 | 4 | 8.000.000 | conteo | `ON_TIME` | 0 |

En los tres mensajes:

- `alert_id`, `idempotency_key` y la clave Kafka son iguales;
- `accumulation_mode = ACCUMULATING`;
- `schema_version = 1.0`;
- `rule_version = velocity-v1`.

El registro normalizado de los mensajes se conserva en `21-alertas-directrunner-final-20260809-190216.log`. La ejecución puede repetirse con el procedimiento descrito en `docs/prueba-desde-cero.md`.
