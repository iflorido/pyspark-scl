FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 \
    PATH="$JAVA_HOME/bin:$PATH"

# Java 21 — compatible con PySpark 3.5.x
RUN apt-get update && apt-get install -y \
    openjdk-21-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dependencias primero (caché de Docker)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Driver JDBC
COPY spark-jars/ ./spark-jars/

# Código fuente
COPY config/conexion.example.py ./config/conexion.example.py
COPY src/  ./src/
COPY web/  ./web/
COPY test_conexion.py .

# Generar conexion.py desde variables de entorno en tiempo de arranque
COPY docker-entrypoint.sh .
RUN chmod +x docker-entrypoint.sh

EXPOSE 5001

CMD ["./docker-entrypoint.sh"]