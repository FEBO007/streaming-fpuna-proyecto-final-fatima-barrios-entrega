# Análisis reproducible de skew y hot keys

Este análisis comprueba cómo la clave Kafka `customer_id` distribuye carga entre las tres particiones de `bank.transactions.raw`. No modifica el pipeline, los tópicos, los umbrales ni la política temporal.

## Pregunta evaluada

¿Qué ocurre cuando muchos clientes aportan una cantidad similar de eventos y qué cambia cuando un solo cliente concentra el 80 % del tráfico?

Se comparan dos escenarios deterministas de igual volumen:

| Escenario | Eventos | Claves únicas | Composición |
|---|---:|---:|---|
| `balanced` | 300 | 300 | Un evento por cliente |
| `hot_key` | 300 | 61 | 240 eventos de `cust-hot-001` y 60 clientes con un evento |

## Simulación offline

Desde la raíz del repositorio, con el entorno virtual activado:

```text
python -m src.producer.skew_analysis
```

La simulación reproduce la asignación CRC32 que el particionador predeterminado `consistent_random` de librdkafka aplica a claves no vacías. El resultado de referencia con tres particiones es:

| Escenario | Partición 0 | Partición 1 | Partición 2 | Mayor participación | Máx./media |
|---|---:|---:|---:|---:|---:|
| `balanced` | 92 (30,67 %) | 116 (38,67 %) | 92 (30,67 %) | 38,67 % | 1,16 |
| `hot_key` | 258 (86,00 %) | 20 (6,67 %) | 22 (7,33 %) | 86,00 % | 2,58 |

La concentración aumenta en **47,33 puntos porcentuales** la participación de la partición más cargada. Los 240 eventos de `cust-hot-001` permanecen juntos en la partición 0 porque una misma clave no puede repartirse entre particiones.

## Medición opcional contra Kafka

El modo `publish` publica ambos escenarios y calcula la distribución a partir de las particiones informadas por los callbacks de entrega reales:

```text
python -m src.producer.skew_analysis --mode publish
```

Debe ejecutarse con Kafka disponible y el pipeline detenido, preferentemente sobre un volumen de laboratorio vacío. El productor declara `partitioner=consistent_random`, la misma política predeterminada de librdkafka usada por el productor del proyecto. La salida conserva el mismo formato que la simulación, pero sus conteos provienen de las entregas confirmadas por Kafka.

## Interpretación

- Tres particiones permiten procesar clientes distintos en paralelo.
- Agregar particiones puede mejorar el paralelismo entre claves, pero no divide una única hot key.
- El orden relativo por `customer_id` se conserva precisamente porque todos sus eventos permanecen en una partición.
- El desequilibrio puede limitar el throughput total aunque las otras particiones tengan capacidad libre.
- Salting o subdividir una clave requeriría una segunda agregación y cambiaría las garantías de orden y estado; no se aplica en este proyecto.

El análisis es una prueba sintética de comportamiento, no un dimensionamiento productivo. Para decidir una mitigación real se necesitarían métricas históricas de volumen por cliente, tamaño de mensajes, lag, latencia y capacidad del runner.

Fuente de la política de particionado: [configuración oficial de librdkafka](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html).
