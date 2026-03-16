# config/conexion.py
import os
from pyspark.sql import SparkSession

# ── Ruta al driver JDBC ─────────────────────────────────────

JAR_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "spark-jars", "mysql-connector-j-8.4.0.jar")
)

# ── Cadena de conexión JDBC ─────────────────────────────────
# useSSL=false          → MariaDB local sin certificado SSL
# serverTimezone=UTC    → evita errores de zona horaria
# allowPublicKeyRetrieval=true → necesario en MySQL/MariaDB 8+
#                                para conexiones remotas
JDBC_URL = (
    "jdbc:mysql://IP:3306/BD_NAME"
    "?useSSL=false"
    "&serverTimezone=UTC"
    "&allowPublicKeyRetrieval=true"
)

# (evita que la password aparezca en logs de Spark)
JDBC_PROPS = {
    "user":     "BD_NAME",
    "password": "BD_PASSWORD",
    "driver":   "com.mysql.cj.jdbc.Driver"
}


# _spark es privado (convención _variable)
_spark = None

def get_spark() -> SparkSession:
    """
    Devuelve siempre la misma SparkSession.
    La primera llamada la crea, las siguientes la reutilizan.
    En producción Spark arranca UNA vez con Flask y sirve
    todas las peticiones sin volver a inicializarse.
    """
    global _spark

    if _spark is None:
        _spark = (SparkSession.builder
            .appName("BancoPySpark")
            # jar del driver MySQL/MariaDB
            .config("spark.jars", JAR_PATH)
            # necesario para que el driver sea visible al ejecutar consultas
            .config("spark.driver.extraClassPath", JAR_PATH)

            .config("spark.ui.enabled", "false")
           
            # si no existe la crea — clave para el patrón singleton
            .getOrCreate())

    return _spark


def leer_tabla(nombre: str):
    """
    Lee una tabla completa de MariaDB en el momento de la llamada.
    Cada vez que se llama obtiene datos actualizados de la BD — sin caché,
    sin ficheros intermedios.
    """
    return get_spark().read.jdbc(
        url=JDBC_URL,
        table=nombre,
        properties=JDBC_PROPS
    )