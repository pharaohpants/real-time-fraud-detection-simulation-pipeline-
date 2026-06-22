FROM apache/spark:3.5.0

USER root

RUN pip install --no-cache-dir kafka-python psycopg2-binary python-dotenv pandas pyyaml

COPY fraud_engine.py /app/fraud_engine.py

CMD ["/opt/spark/bin/spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0", \
     "/app/fraud_engine.py"]
