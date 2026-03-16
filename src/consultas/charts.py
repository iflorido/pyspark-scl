# src/consultas/charts.py
# =============================================================
#  Consultas PySpark para alimentar las 8 gráficas del
#  dashboard de analytics bancario.
#  Cada función devuelve datos agregados listos para Chart.js.
# =============================================================

import sys
import os

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT_DIR)

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from config.conexion import leer_tabla


def df_to_records(df):
    return df.toPandas().fillna(0).to_dict("records")


# ── G1: Distribución de ofertas de tarjeta ───────────────────
# Pie chart — cuántos clientes caen en cada categoría
def chart_distribucion_tarjetas():
    clientes = leer_tabla("Clientes")
    cuentas  = leer_tabla("Cuentas")

    df = (
        cuentas.filter(F.col("activa") == 1)
        .join(clientes, "id_cliente")
        .withColumn("oferta",
            F.when(F.col("saldo") > 50000,  "Premium")
             .when(F.col("saldo") >= 10000, "Oro")
             .otherwise("Estándar"))
        .groupBy("oferta")
        .agg(F.count("*").alias("total"))
        .orderBy(F.desc("total"))
    )
    return df_to_records(df)


# ── G2: Saldo total por sucursal ─────────────────────────────
# Bar chart horizontal — ranking de sucursales por volumen
def chart_saldo_por_sucursal():
    sucursales = leer_tabla("Sucursales")
    clientes   = leer_tabla("Clientes")
    cuentas    = leer_tabla("Cuentas")

    sucursales = sucursales.withColumnRenamed("nombre", "sucursal")

    df = (
        sucursales
        .join(clientes.filter(F.col("activo") == 1), "id_sucursal", how="left")
        .join(cuentas.filter(F.col("activa") == 1), "id_cliente", how="left")
        .groupBy("id_sucursal", "sucursal")
        .agg(
            F.round(F.sum("saldo"), 2).alias("saldo_total"),
            F.countDistinct("id_cliente").alias("num_clientes")
        )
        .orderBy(F.desc("saldo_total"))
        .limit(10)
    )
    return df_to_records(df)


# ── G3: Distribución de riesgo crediticio ───────────────────
# Donut chart — peso de cada nivel de riesgo sobre el saldo total
def chart_distribucion_riesgo():
    cuentas = leer_tabla("Cuentas")
    score   = leer_tabla("ScoreCliente")

    resumen = (
        score
        .join(cuentas.filter(F.col("activa") == 1), "id_cliente")
        .groupBy("riesgo")
        .agg(
            F.count("*").alias("num_clientes"),
            F.round(F.sum("saldo"), 2).alias("saldo_total")
        )
    )

    ventana_global = Window.rowsBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )

    df = (
        resumen
        .withColumn("pct",
            F.round(F.col("saldo_total") * 100.0 /
                    F.sum("saldo_total").over(ventana_global), 1))
        .orderBy(F.desc("saldo_total"))
    )
    return df_to_records(df)


# ── G4: Clientes por segmento ────────────────────────────────
# Bar chart — distribución de la base de clientes por segmento
def chart_clientes_por_segmento():
    clientes = leer_tabla("Clientes")
    cuentas  = leer_tabla("Cuentas")

    df = (
        clientes.filter(F.col("activo") == 1)
        .join(cuentas.filter(F.col("activa") == 1), "id_cliente")
        .groupBy("segmento")
        .agg(
            F.countDistinct("id_cliente").alias("num_clientes"),
            F.round(F.avg("saldo"), 2).alias("saldo_medio")
        )
        .orderBy(F.desc("num_clientes"))
    )
    return df_to_records(df)


# ── G5: Score crediticio medio por segmento ──────────────────
# Bar chart — calidad crediticia de cada segmento
def chart_score_por_segmento():
    clientes = leer_tabla("Clientes")
    score    = leer_tabla("ScoreCliente")

    df = (
        clientes.filter(F.col("activo") == 1)
        .join(score, "id_cliente")
        .groupBy("segmento")
        .agg(
            F.round(F.avg("score_credito"), 1).alias("score_medio"),
            F.round(F.min("score_credito"), 1).alias("score_min"),
            F.round(F.max("score_credito"), 1).alias("score_max")
        )
        .orderBy(F.desc("score_medio"))
    )
    return df_to_records(df)


# ── G6: Propensión a productos por segmento ──────────────────
# Radar / grouped bar — qué segmentos tienen más propensión
def chart_propension_por_segmento():
    clientes = leer_tabla("Clientes")
    score    = leer_tabla("ScoreCliente")

    df = (
        clientes.filter(F.col("activo") == 1)
        .join(score, "id_cliente")
        .groupBy("segmento")
        .agg(
            F.round(F.avg(F.col("propension_ahorro")   .cast("int")) * 100, 1).alias("pct_ahorro"),
            F.round(F.avg(F.col("propension_inversion").cast("int")) * 100, 1).alias("pct_inversion"),
            F.round(F.avg(F.col("propension_seguro")   .cast("int")) * 100, 1).alias("pct_seguro")
        )
        .orderBy("segmento")
    )
    return df_to_records(df)


# ── G7: Top 10 provincias por saldo medio ───────────────────
# Bar chart horizontal — geografía del dinero
def chart_saldo_por_provincia():
    clientes   = leer_tabla("Clientes")
    cuentas    = leer_tabla("Cuentas")
    provincias = leer_tabla("Provincias")

    provincias = provincias.withColumnRenamed("nombre", "provincia")

    df = (
        clientes.filter(F.col("activo") == 1)
        .join(cuentas.filter(F.col("activa") == 1), "id_cliente")
        .join(provincias, "id_provincia")
        .groupBy("provincia")
        .agg(
            F.round(F.avg("saldo"), 2).alias("saldo_medio"),
            F.round(F.sum("saldo"), 2).alias("saldo_total"),
            F.countDistinct("id_cliente").alias("num_clientes")
        )
        .orderBy(F.desc("saldo_total"))
        .limit(10)
    )
    return df_to_records(df)


# ── G8: Morosos vs activos por sucursal ──────────────────────
# Stacked bar — calidad de la cartera de préstamos
def chart_prestamos_por_estado():
    sucursales = leer_tabla("Sucursales")
    clientes   = leer_tabla("Clientes")
    prestamos  = leer_tabla("Prestamos")

    sucursales = sucursales.withColumnRenamed("nombre", "sucursal")

    df = (
        sucursales
        .join(clientes, "id_sucursal", how="left")
        .join(prestamos, "id_cliente", how="left")
        .groupBy("sucursal", "estado")
        .agg(F.count("id_prestamo").alias("total"))
        .filter(F.col("estado").isNotNull())
        .orderBy("sucursal", "estado")
    )
    return df_to_records(df)


# ── Todas las gráficas de una vez ────────────────────────────
def todas_las_graficas():
    return {
        "tarjetas":          chart_distribucion_tarjetas(),
        "saldo_sucursal":    chart_saldo_por_sucursal(),
        "riesgo":            chart_distribucion_riesgo(),
        "segmento_clientes": chart_clientes_por_segmento(),
        "score_segmento":    chart_score_por_segmento(),
        "propension":        chart_propension_por_segmento(),
        "saldo_provincia":   chart_saldo_por_provincia(),
        "prestamos_estado":  chart_prestamos_por_estado(),
    }