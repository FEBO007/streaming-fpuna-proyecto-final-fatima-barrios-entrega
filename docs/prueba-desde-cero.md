# Procedimiento de reproducción y verificación independiente

Esta guía permite verificar el proyecto sin depender del entorno, los offsets ni los archivos locales de la autora. El procedimiento parte de un clon nuevo y separa dos niveles:

1. **Verificación del código:** instalación, pruebas, calidad, Compose e integridad de evidencias.
2. **Validación end-to-end:** productor sintético -> Kafka -> Apache Beam -> Kafka -> materializador.

## 1. Preparar un clon limpio

Use una carpeta nueva y clone el repositorio público:

```text
git clone https://github.com/FEBO007/streaming-fpuna-proyecto-final-fatima-barrios-entrega.git
cd streaming-fpuna-proyecto-final-fatima-barrios-entrega
git status --short
git rev-parse HEAD
```

`git status --short` no debe mostrar archivos. El hash permite registrar exactamente qué versión fue evaluada.

### Windows PowerShell

```powershell
py -3.14 -m venv .venv
Set-ExecutionPolicy -Scope Process Bypass
& .\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

### Linux o macOS

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
```

El proyecto admite Python 3.12, 3.13 y 3.14.

## 2. Ejecutar la verificación en un comando

Con el entorno virtual activado:

```text
python scripts/verify_project.py --require-clean
```

El mismo comando funciona en PowerShell, Bash y zsh. Verifica:

- suite completa de `pytest`;
- lint con Ruff;
- formato con Ruff;
- sintaxis y resolución de `compose.yaml`;
- SHA-256 de cada archivo inventariado como evidencia;
- versión de Python, commit y limpieza del clon.

El final esperado es:

```text
EVIDENCE_CHECKSUMS_VERIFIED=5
COMPOSE_CONFIG=VALID
VERIFICATION_EXIT_CODE=0
```

Si se desea revisar solo el código en un entorno que no tiene Docker, puede usarse `--skip-compose`. Esa variante no sustituye la verificación completa.

### 2.1 Comprobar distribución y hot keys

Este análisis es local y no requiere Kafka:

```text
python -m src.producer.skew_analysis
```

Compara 300 clientes con un evento cada uno frente a 300 eventos donde `cust-hot-001` concentra 240. Los resultados de referencia son 92/116/92 eventos para el escenario balanceado y 258/20/22 para el escenario con hot key. La mayor participación pasa de 38,67 % a 86,00 % y el proceso termina con:

```text
CONCLUSION more_partitions_distribute_distinct_keys_but_do_not_split_one_hot_key
SKEW_ANALYSIS_EXIT_CODE=0
```

El modo opcional `--mode publish` mide las particiones confirmadas por Kafka. Debe ejecutarse con el pipeline detenido y preferentemente sobre un volumen de laboratorio vacío; no forma parte de la validación de referencia de ocho eventos.

## 3. Recrear Kafka desde un estado vacío

Requisitos adicionales para la validación end-to-end:

- Docker Desktop o Docker Engine con Compose;
- Java/JDK 17 en `PATH`;
- aproximadamente 4 GB de memoria libre.

El siguiente comando elimina exclusivamente los contenedores y el volumen declarados por este proyecto. No debe ejecutarse si se desea conservar una ejecución anterior:

```text
docker compose down -v
docker compose up -d kafka kafka-init
docker compose ps -a
```

Antes de continuar, `kafka` debe aparecer saludable y `kafka-init` debe haber terminado con código 0.

## 4. Ejecutar el recorrido end-to-end

Abra tres terminales en la raíz del mismo clon, con el entorno virtual activado.

### Terminal A - Apache Beam

Ejecute en una sola línea:

```text
python -m src.pipeline.streaming_pipeline --bootstrap-servers localhost:9092 --input-topic bank.transactions.raw --alert-topic risk.alerts.velocity --invalid-topic bank.transactions.invalid --consumer-group velocity-beam-validation --max-num-records 8 --kafka-environment-type DOCKER --kafka-environment-config apache/beam_java17_sdk:2.75.0 --directrunner-smoke-mode --runner DirectRunner --environment_type LOOPBACK --job_name velocity-beam-validation
```

El pipeline queda esperando exactamente ocho registros y luego finaliza.

### Terminal B - productor sintético

```text
python -m src.producer.synthetic_producer --base-time 2026-08-09T19:59:50Z --interval-seconds 0.10 --cycles 1
```

La salida debe mostrar ocho publicaciones. El escenario incluye un duplicado intencional, dos eventos fuera de orden y un candidato tardío.

### Terminal C - salida materializada

Espere a que la terminal A termine y ejecute:

```text
python -m src.materializer.alert_consumer --bootstrap-servers localhost:9092 --topic risk.alerts.velocity --consumer-group velocity-alert-validation --from-beginning --max-messages 3 --timeout-seconds 10 --state-db data/materialized-alerts.sqlite3
```

El cierre esperado es:

```text
MATERIALIZED_KEYS=3 MESSAGES_READ=3 MATERIALIZED_UPDATES=3 STATE_DB=data/materialized-alerts.sqlite3
```

Para comprobar persistencia e idempotencia después de reiniciar el proceso, repita el consumo con otro grupo y el mismo archivo SQLite:

```text
python -m src.materializer.alert_consumer --bootstrap-servers localhost:9092 --topic risk.alerts.velocity --consumer-group velocity-alert-replay --from-beginning --max-messages 3 --timeout-seconds 10 --state-db data/materialized-alerts.sqlite3
```

La segunda ejecución relee los tres mensajes, conserva las mismas tres claves y termina con `MATERIALIZED_UPDATES=0`: ninguna revisión duplicada vuelve a modificar la vista.

## 5. Verificar offsets y resultado

```text
docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:29092 --topic bank.transactions.raw
docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:29092 --topic risk.alerts.velocity
docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:29092 --topic bank.transactions.invalid
```

Resultado de referencia:

| Control | Resultado esperado |
|---|---:|
| Registros de entrada | 8 |
| Alertas | 3 |
| Inválidos | 0 |
| Claves materializadas | 3 |
| Actualizaciones al repetir | 0 |

Las alertas deben corresponder a estas ventanas UTC:

| Ventana | Conteo | Total PYG | Condición |
|---|---:|---:|---|
| 19:59:40-20:00:40 | 4 | 8.000.000 | conteo |
| 19:59:50-20:00:50 | 5 | 10.500.000 | conteo y monto |
| 20:00:00-20:01:00 | 4 | 8.000.000 | conteo |

El orden de los tres mensajes puede variar. Para cada alerta, la clave Kafka, `alert_id` e `idempotency_key` deben coincidir.

## 6. Qué demuestra cada prueba

| Evidencia | Qué permite concluir |
|---|---|
| `pytest` | Contrato, validación, event time, ventanas, deduplicación, agregación y alertas. |
| TestStream | Pane `ON_TIME`, corrección `LATE`, duplicado ignorado y descarte fuera del lateness. |
| Productor real | Ocho registros, clave por cliente, timestamp Kafka basado en el evento y escenario adverso reproducible. |
| Análisis de skew | Distribución entre tres particiones y concentración causada por una hot key. |
| KafkaIO end-to-end | Recorrido real Kafka -> Beam/DirectRunner -> Kafka. |
| Materializador SQLite | Tres claves persistentes, reinicio y `upsert` solo de la mayor revisión observada. |
| SHA-256 | Integridad de los logs y del resumen entregados. |

Esta prueba demuestra una solución local reproducible con semántica **at-least-once tolerante a duplicados y efectos idempotentes**. El consumidor persiste antes de confirmar el offset; un replay puede repetir el mensaje, pero no el efecto lógico. No demuestra exactly-once end-to-end, alta disponibilidad ni recuperación distribuida ante fallos.

## 7. Cerrar el entorno

Para conservar el volumen de Kafka:

```text
docker compose down
```

Para eliminar también los datos de esta validación:

```text
docker compose down -v
```
