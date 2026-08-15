# Entrega final

Proyecto integrador de **Streaming de datos y sus aplicaciones** — Fátima Barrios Ortega.

## Contenido principal

- `README.md`: instalación, ejecución, decisiones, garantías y límites.
- `compose.yaml`: Kafka y creación determinista de tópicos.
- `src/`: productor, pipeline Beam y consumidor con materialización durable en SQLite.
- `tests/`: pruebas unitarias, de integración y TestStream.
- `scripts/`: verificación multiplataforma y ejecución reproducible en PowerShell.
- `docs/prueba-desde-cero.md`: procedimiento de reproducción y verificación independiente desde un clon limpio.
- `docs/analisis-skew.md`: comparación reproducible de distribución normal y hot key.
- `docs/documento-tecnico-streaming-fatima-barrios.docx`: documento técnico editable.
- `docs/documento-tecnico-streaming-fatima-barrios.pdf`: documento técnico final.
- `docs/architecture/`: diagrama y fuentes editables.
- `docs/presentation/`: presentación en formatos PPTX y PDF.
- `evidence/`: inventario y resultados de verificación automatizada, end-to-end y de distribución.

## Estado de validación

- 71 pruebas automatizadas superadas.
- Análisis reproducible: 300 clientes distribuidos frente a una hot key con 80 % de la carga.
- Ruff y verificación de formato sin hallazgos.
- Compose limitado a `kafka` y `kafka-init`.
- Validación end-to-end: 8 eventos consumidos, 3 alertas, 0 inválidos, lag final 0 y código de salida 0.
- Entorno de ejecución documentado: Apache Beam DirectRunner.
