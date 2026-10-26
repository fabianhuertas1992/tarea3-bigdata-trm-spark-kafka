# Análisis histórico de la TRM con PySpark y práctica de Kafka

Autor: Fabián Andrés Huertas Reyes. UNAD, Big Data, grupo 106. Octubre de 2026.

El proyecto estudia promedios mensuales y variaciones entre cotizaciones de la TRM de Colombia. La práctica de streaming corresponde a sensores simulados del Anexo 3. Las dos fuentes se presentan por separado.

## Alcance

La descarga del 25 de octubre de 2026 contiene 8.374 registros originales y cuatro columnas. Este tamaño permite demostrar el proceso, pero no constituye por sí solo una prueba de rendimiento a gran escala. El análisis es descriptivo; no incluye predicción financiera ni pruebas de carga.

## Archivos

- `codigo/descargar_trm.py`: descarga paginada de la API oficial y procedencia SHA256.
- `codigo/batch_trm.py`: limpieza, serie diaria según vigencias, Spark SQL, variaciones y comprobación RDD.
- `codigo/kafka_producer.py`: envía sensores simulados y conserva eventos confirmados.
- `codigo/spark_streaming_consumer.py`: agrega por sensor y ventanas de un minuto.
- `codigo/visualizar_resultados.py`: gráficos de las salidas de Spark.
- `codigo/validar_resultados.py`: comparación independiente de promedios y conteos.
- `datos/` y `resultados/`: carpetas locales generadas al ejecutar el proyecto; no se incluyen en este repositorio.
- El informe, los datos originales y las capturas de la ejecución se conservan como entrega académica separada.

## Entorno utilizado

Ubuntu ARM64 de VirtualBox BIGDATA, Java 11, Hadoop 3.3.6, Spark 3.5.8 con Scala 2.12, Kafka 3.9.2, kafka-python 2.3.0. Batch usa Spark Standalone; streaming usa local[2] dentro de Ubuntu para limitar memoria. El laboratorio tiene una VM, una partición Kafka y replicación uno: no prueba alta disponibilidad multinodo.

## Ejecución desde una copia nueva

Los comandos de esta sección se ejecutan en **Ubuntu**, dentro de la raíz del repositorio. Requieren Python 3.10 y Java 11 instalados. Para el laboratorio original se usó Ubuntu 22.04 ARM64. No es necesario usar HDFS para esta alternativa local.

```bash
git clone https://github.com/fabianhuertas1992/tarea3-bigdata-trm-spark-kafka.git
cd tarea3-bigdata-trm-spark-kafka
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python codigo/descargar_trm.py --salida datos
spark-submit --master 'local[2]' codigo/batch_trm.py --entrada datos/trm_original.csv --fecha-corte 2026-10-25 --salida resultados/batch
```

La API cambia con el tiempo: una descarga nueva puede contener más registros que la instantánea del informe. El corte limita las fechas analizadas, pero los conteos de registros originales pueden cambiar. Conserva `datos/procedencia.json` para identificar la descarga.

Para Kafka se requiere `curl` y `tar`. Descarga y verifica la distribución oficial:

```bash
mkdir -p runtime
cd runtime
curl -fLO https://archive.apache.org/dist/kafka/3.9.2/kafka_2.12-3.9.2.tgz
curl -fLO https://archive.apache.org/dist/kafka/3.9.2/kafka_2.12-3.9.2.tgz.sha512
sha512sum -c kafka_2.12-3.9.2.tgz.sha512
# Continúa solamente si la verificación indica OK.
tar -xzf kafka_2.12-3.9.2.tgz
cd ..
bash codigo/iniciar_kafka.sh
```

`iniciar_kafka.sh` utiliza Java 11 en `/usr/lib/jvm/java-11-openjdk-arm64`, como en la VM original. En Ubuntu x86_64 ajusta esa ruta a tu instalación de Java 11 antes de ejecutarlo. Los puertos 2181 y 9092 deben estar libres para este laboratorio.

En una copia nueva y con el topic `sensor_data` vacío, abre dos terminales **dentro de Ubuntu** y sitúa ambas en la raíz del repositorio. Activa `.venv` en ambas.

Terminal A (inicia primero el consumidor):

```bash
source .venv/bin/activate
spark-submit --master 'local[2]' --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 codigo/spark_streaming_consumer.py --topic sensor_data --duracion 240 --salida resultados/streaming --checkpoint runtime/checkpoint_sensor
```

Terminal B (espera a que el consumidor esté escuchando):

```bash
source .venv/bin/activate
python codigo/kafka_producer.py --topic sensor_data --eventos 120 --intervalo 1 --salida resultados/streaming/productor.jsonl
```

Después de que termine el consumidor:

```bash
python codigo/validar_resultados.py
python codigo/visualizar_resultados.py
bash codigo/detener_kafka.sh
```

Esta primera ejecución utiliza las rutas esperadas por el validador. No vuelvas a enviar eventos al mismo topic para reproducir esos conteos: crea un topic, salida y checkpoint nuevos y conserva las salidas anteriores. Las instrucciones siguientes describen los accesos y scripts auxiliares de la **VM original**, que dependen de su configuración previa (`~/.spark-env`, HDFS y `~/venvs/spark`).

## Dónde ejecutar en la VM original

En la Mac se abre Terminal y se conecta a Ubuntu:

```sh
ssh -p 2222 vboxuser@127.0.0.1
```

Los comandos siguientes se ejecutan dentro de Ubuntu, con el indicador `vboxuser@bigdata`:

```sh
cd ~/tarea3_trm
source ~/.spark-env
```

La copia en Drive sirve para editar, revisar y respaldar. Las rutas de Ubuntu empiezan por `/home/vboxuser/tarea3_trm`; no son rutas de la Mac.

## Datos y batch

Fuente oficial: https://www.datos.gov.co/d/mcec-87by

Para descargar una nueva instantánea sin sobrescribir la utilizada en el informe:

```sh
python3 codigo/descargar_trm.py --salida datos/nueva_consulta
```

El CSV ejecutado está en HDFS en `/Tarea3_TRM_2026/trm_original.csv`. Para repetir el análisis de la instantánea del informe:

```sh
bash codigo/ejecutar_batch.sh
```

El script produce los CSV y `resultados/batch/resumen.json`. Los promedios mensuales se ponderan por días calendario: cada tasa se aplica a las fechas entre `vigenciadesde` y `vigenciahasta`. El corte es el 25 de octubre, incluso si el último registro tiene vigencia posterior. Las variaciones porcentuales comparan cotizaciones consecutivas y no necesariamente días consecutivos. No son rendimientos de una inversión.

Los resultados se recogen al driver para exportarlos porque el conjunto es pequeño. Para datos grandes deben escribirse distribuidamente; el cálculo global de lag requiere también revisar su particionamiento. No se afirma haber realizado un benchmark.

## Kafka y streaming

La distribución oficial y su checksum están en `runtime` de Ubuntu. Para una instalación nueva, descargar ambos archivos desde `https://archive.apache.org/dist/kafka/3.9.2/`, verificar SHA512 y extraer `kafka_2.12-3.9.2.tgz` en `runtime`. ZooKeeper y Kafka escuchan solamente en la VM en los puertos locales 2181 y 9092.

```sh
bash codigo/iniciar_kafka.sh
bash codigo/preparar_demostracion.sh
```

Cada demostración crea un topic, una carpeta y un checkpoint nuevos para conservar los resultados originales. Abrir dos terminales de la Mac y conectar ambas por SSH a Ubuntu.

Terminal A dentro de Ubuntu:

```sh
cd ~/tarea3_trm
bash codigo/ejecutar_consumidor.sh
```

Terminal B dentro de Ubuntu:

```sh
cd ~/tarea3_trm
bash codigo/ejecutar_productor.sh
```

El productor envía 120 eventos a intervalos de un segundo. El consumidor se detiene a los 240 segundos. Muestra conteos y promedios de temperatura y humedad por sensor y minuto; conserva cada actualización como CSV. Las ventanas se expresan en UTC. El watermark es de dos minutos; no se simulan atrasos superiores a ese límite. No se usan dispositivos físicos ni se presentan estas lecturas como TRM en vivo.

## Verificación de la ejecución original

Desde la raíz del proyecto, con Python y matplotlib instalados:

```sh
python3 codigo/validar_resultados.py
python3 codigo/visualizar_resultados.py
```

La validación independiente reproduce los promedios mensuales y compara las últimas actualizaciones de cada ventana con los eventos confirmados por el productor. No se suman todos los CSV de microbatches porque son actualizaciones de estado, no grupos disjuntos.

## Resultados de la ejecución documentada

La instantánea del 25 de octubre de 2026 produjo 8.374 registros, 12.747 fechas diarias y 419 promedios mensuales verificados. Kafka confirmó 120 eventos y la agregación recuperó los mismos 120 eventos en 30 pares ventana-sensor. El número de ventanas depende del momento de inicio; una ejecución nueva puede distribuir los eventos en otra cantidad de ventanas.

Presentación: https://canva.link/clt8urr4ft1yudp

Repositorio: https://github.com/fabianhuertas1992/tarea3-bigdata-trm-spark-kafka

## Referencias técnicas

- https://spark.apache.org/docs/3.5.8/structured-streaming-kafka-integration.html
- https://spark.apache.org/docs/3.5.8/structured-streaming-programming-guide.html
- https://spark.apache.org/docs/3.5.8/rdd-programming-guide.html
- https://kafka.apache.org/39/getting-started/quickstart/
