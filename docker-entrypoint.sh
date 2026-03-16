#!/bin/bash
# docker-entrypoint.sh
# Genera config/conexion.py desde variables de entorno

cat > /app/config/conexion.py << EOF
import os
from pyspark.sql import SparkSession

JAR_PATH = "/app/spark-jars/mysql-connector-j-8.4.0.jar"

JDBC_URL = "jdbc:mysql://${DB_HOST}:${DB_PORT:-3306}/${DB_NAME}?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"

JDBC_PROPS = {
    "user":     "${DB_USER}",
    "password": "${DB_PASSWORD}",
    "driver":   "com.mysql.cj.jdbc.Driver"
}

_spark = None

def get_spark():
    global _spark
    if _spark is None:
        _spark = (SparkSession.builder
            .appName("BancoPySpark")
            .config("spark.jars", JAR_PATH)
            .config("spark.driver.extraClassPath", JAR_PATH)
            .config("spark.ui.enabled", "false")
            .getOrCreate())
    return _spark

def leer_tabla(nombre: str):
    return get_spark().read.jdbc(
        url=JDBC_URL,
        table=nombre,
        properties=JDBC_PROPS
    )
EOF

exec python web/app.py