# test_conexion.py (en la raíz del proyecto)
from config.conexion import get_spark, JDBC_URL, JDBC_PROPS

spark = get_spark()

print("\n🔌 Probando conexión a Base de datos en IP...\n")

tablas = ["Clientes", "Cuentas", "Prestamos", "ScoreCliente", "Sucursales", "Provincias"]

for tabla in tablas:
    try:
        df = spark.read.jdbc(url=JDBC_URL, table=tabla, properties=JDBC_PROPS)
        print(f"✅ {tabla:<20} — {df.count()} filas")
    except Exception as e:
        print(f"❌ {tabla:<20} — Error: {e}")

print("\n📋 Schema de Clientes:")
spark.read.jdbc(url=JDBC_URL, table="Clientes", properties=JDBC_PROPS).printSchema()

print("\n✅ Test completado.")
spark.stop()