# src/consultas/ejecutores.py
# =============================================================
#  Funciones PySpark para cada consulta del catálogo.
#  Cada función lee datos frescos de MariaDB en el momento
#  de ser llamada — sin caché, sin ficheros intermedios.
#  El MAPA al final conecta cada id con su función.
# =============================================================

import sys
import os

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT_DIR)

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from config.conexion import leer_tabla


# ── Helper ───────────────────────────────────────────────────
def df_to_records(df):
    """
    Convierte DataFrame Spark → lista de dicts.
    toPandas() trae los datos al driver.
    to_dict("records") los convierte a formato JSON-serializable.
    fillna("") evita errores de serialización con valores None.
    """
    return df.toPandas().fillna("").to_dict("records")


# ══════════════════════════════════════════════════════════════
#  BLOQUE A — CASE / Clasificación
# ══════════════════════════════════════════════════════════════

def a1_oferta_tarjeta():
    clientes = leer_tabla("Clientes")
    cuentas  = leer_tabla("Cuentas")

    df = (
        cuentas.filter(F.col("activa") == 1)
        .join(clientes, "id_cliente")
        .withColumn("oferta_recomendada",
            F.when(F.col("saldo") > 50000,  "Tarjeta Premium")
             .when(F.col("saldo") >= 10000, "Tarjeta Oro")
             .otherwise("Tarjeta Estándar"))
        .select(
            "id_cuenta",
            F.concat(F.col("nombre"), F.lit(" "), F.col("apellidos")).alias("cliente"),
            "tipo_cuenta",
            F.round("saldo", 2).alias("saldo"),
            "oferta_recomendada")
        .limit(25)
    )
    return df_to_records(df)


def a2_scoring_multiproducto():
    clientes = leer_tabla("Clientes")
    cuentas  = leer_tabla("Cuentas")
    score    = leer_tabla("ScoreCliente")

    df = (
        clientes.filter(F.col("activo") == 1)
        .join(cuentas.filter(F.col("activa") == 1), "id_cliente")
        .join(score, "id_cliente")
        .withColumn("edad",
            F.floor(F.datediff(F.current_date(), F.col("fecha_nacimiento")) / 365))
        .withColumn("tarjeta",
            F.when(F.col("saldo") > 50000, "Tarjeta Premium")
             .when(F.col("saldo") > 10000, "Tarjeta Oro")
             .otherwise("Tarjeta Estándar"))
        .withColumn("fondo",
            F.when(
                (F.col("propension_inversion") == 1) & (F.col("saldo") > 5000),
                "Fondo Renta Variable")
             .when(
                (F.col("propension_ahorro") == 1) & (F.col("saldo") > 1000),
                "Fondo Renta Fija")
             .otherwise("—"))
        .withColumn("seguro",
            F.when(F.col("propension_seguro") == 1, "Seguro Vida Premium")
             .otherwise("Seguro Hogar"))
        .select(
            "id_cliente",
            F.concat(F.col("nombre"), F.lit(" "), F.col("apellidos")).alias("cliente"),
            "segmento",
            F.round("saldo", 2).alias("saldo"),
            "edad", "tarjeta", "fondo", "seguro")
        .orderBy(F.desc("saldo"))
        .limit(25)
    )
    return df_to_records(df)


# ══════════════════════════════════════════════════════════════
#  BLOQUE B — Window Functions
# ══════════════════════════════════════════════════════════════

def b1_ranking_comparado():
    clientes = leer_tabla("Clientes")
    cuentas  = leer_tabla("Cuentas")

    ventana = Window.partitionBy("id_sucursal").orderBy(F.desc("saldo"))

    df = (
        clientes
        .join(cuentas.filter(F.col("activa") == 1), "id_cliente")
        .withColumn("rnk",       F.rank()      .over(ventana))
        .withColumn("dense_rnk", F.dense_rank().over(ventana))
        .withColumn("row_num",   F.row_number().over(ventana))
        .select(
            "id_sucursal",
            "id_cliente",
            F.concat(F.col("nombre"), F.lit(" "), F.col("apellidos")).alias("cliente"),
            F.round("saldo", 2).alias("saldo"),
            "rnk", "dense_rnk", "row_num")
        .orderBy("id_sucursal", F.desc("saldo"))
        .limit(30)
    )
    return df_to_records(df)


def b2_top3_provincia():
    clientes   = leer_tabla("Clientes")
    cuentas    = leer_tabla("Cuentas")
    provincias = leer_tabla("Provincias")

    ventana = Window.partitionBy("id_provincia").orderBy(F.desc("saldo"))
    provincias = provincias.withColumnRenamed("nombre", "provincia")
    df = (
        clientes
        .join(cuentas.filter(F.col("activa") == 1), "id_cliente")
        .join(provincias, "id_provincia")
        .withColumn("ranking", F.rank().over(ventana))
        .filter(F.col("ranking") <= 3)
        .select(
            "id_cliente",
            F.concat(F.col("nombre"), F.lit(" "), F.col("apellidos")).alias("cliente"),
            "provincia",
            F.round("saldo", 2).alias("saldo"),
            "ranking")
        .orderBy("provincia", "ranking")
        .limit(30)
    )
    return df_to_records(df)


def b3_lag_saldos():
    clientes = leer_tabla("Clientes")
    cuentas  = leer_tabla("Cuentas")

    ventana = Window.partitionBy("id_sucursal").orderBy(F.desc("saldo"))

    df = (
        clientes
        .join(cuentas.filter(F.col("activa") == 1), "id_cliente")
        .withColumn("saldo_anterior", F.lag("saldo").over(ventana))
        .withColumn("diferencia",
            F.round(F.col("saldo") - F.col("saldo_anterior"), 2))
        .select(
            "id_sucursal",
            F.concat(F.col("nombre"), F.lit(" "), F.col("apellidos")).alias("cliente"),
            F.round("saldo", 2).alias("saldo"),
            F.round("saldo_anterior", 2).alias("saldo_anterior"),
            "diferencia")
        .orderBy("id_sucursal", F.desc("saldo"))
        .limit(25)
    )
    return df_to_records(df)


# ══════════════════════════════════════════════════════════════
#  BLOQUE C — JOINs
# ══════════════════════════════════════════════════════════════

def c1_sin_prestamo():
    clientes  = leer_tabla("Clientes")
    cuentas   = leer_tabla("Cuentas")
    prestamos = leer_tabla("Prestamos")

    prestamos_activos = (
        prestamos.filter(F.col("estado") == "Activo")
        .select("id_cliente")
        .distinct()
    )

    df = (
        clientes.filter(F.col("activo") == 1)
        .join(cuentas.filter(F.col("activa") == 1), "id_cliente")
        .join(prestamos_activos, "id_cliente", how="left_anti")
        .select(
            "id_cliente",
            F.concat(F.col("nombre"), F.lit(" "), F.col("apellidos")).alias("cliente"),
            "segmento",
            F.round("saldo", 2).alias("saldo"))
        .orderBy(F.desc("saldo"))
        .limit(25)
    )
    return df_to_records(df)


def c2_metricas_sucursales():
    sucursales = leer_tabla("Sucursales")
    provincias = leer_tabla("Provincias")
    clientes   = leer_tabla("Clientes")
    cuentas    = leer_tabla("Cuentas")
    prestamos  = leer_tabla("Prestamos")

    df = (
        sucursales
        .join(provincias, "id_provincia")
        .join(clientes.filter(F.col("activo") == 1),
              "id_sucursal", how="left")
        .join(cuentas.filter(F.col("activa") == 1),
              "id_cliente", how="left")
        .join(
            prestamos.filter(F.col("estado") == "Activo")
                     .select("id_cliente", "id_prestamo"),
            "id_cliente", how="left")
        .groupBy(
            sucursales["id_sucursal"],
            sucursales["nombre"].alias("sucursal"),
            provincias["nombre"].alias("provincia"))
        .agg(
            F.countDistinct("id_cliente").alias("num_clientes"),
            F.round(F.avg("saldo"), 2).alias("saldo_medio"),
            F.round(F.sum("saldo"), 2).alias("saldo_total"),
            F.countDistinct("id_prestamo").alias("prestamos_activos"))
        .orderBy(F.desc("saldo_total"))
    )
    return df_to_records(df)


# ══════════════════════════════════════════════════════════════
#  BLOQUE D — Subconsultas
# ══════════════════════════════════════════════════════════════

def d1_media_segmento():
    clientes = leer_tabla("Clientes")
    cuentas  = leer_tabla("Cuentas")

    # Subconsulta correlacionada → precalcular media por segmento
    media_segmento = (
        clientes.join(cuentas, "id_cliente")
        .groupBy("segmento")
        .agg(F.round(F.avg("saldo"), 2).alias("media_segmento"))
    )

    df = (
        clientes.filter(F.col("activo") == 1)
        .join(cuentas, "id_cliente")
        .join(media_segmento, "segmento")
        .filter(F.col("saldo") > F.col("media_segmento"))
        .select(
            "id_cliente",
            F.concat(F.col("nombre"), F.lit(" "), F.col("apellidos")).alias("cliente"),
            "segmento",
            F.round("saldo", 2).alias("saldo"),
            "media_segmento")
        .orderBy("segmento", F.desc("saldo"))
        .limit(25)
    )
    return df_to_records(df)


# ══════════════════════════════════════════════════════════════
#  BLOQUE E — CTEs
# ══════════════════════════════════════════════════════════════

def e1_cte_top_clientes():
    clientes = leer_tabla("Clientes")
    cuentas  = leer_tabla("Cuentas")
    score    = leer_tabla("ScoreCliente")

    # CTE 1
    saldo_total = (
        cuentas.filter(F.col("activa") == 1)
        .groupBy("id_cliente")
        .agg(F.round(F.sum("saldo"), 2).alias("total"))
    )

    # CTE 2
    top_clientes = (
        clientes.filter(F.col("activo") == 1)
        .join(saldo_total, "id_cliente")
        .filter(F.col("total") > 30000)
        .select(
            "id_cliente",
            F.concat(F.col("nombre"), F.lit(" "), F.col("apellidos")).alias("cliente"),
            "segmento", "total")
    )

    # SELECT final
    df = (
        top_clientes.join(score, "id_cliente")
        .select("id_cliente", "cliente", "segmento",
                "total", "score_credito", "riesgo")
        .orderBy(F.desc("total"))
        .limit(25)
    )
    return df_to_records(df)


def e2_distribucion_riesgo():
    cuentas = leer_tabla("Cuentas")
    score   = leer_tabla("ScoreCliente")

    resumen = (
        score
        .join(cuentas.filter(F.col("activa") == 1), "id_cliente")
        .groupBy("riesgo")
        .agg(
            F.count("*").alias("num_clientes"),
            F.round(F.avg("saldo"), 2).alias("saldo_medio"),
            F.round(F.sum("saldo"), 2).alias("saldo_total"))
    )

    ventana_global = Window.rowsBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )

    df = (
        resumen
        .withColumn("pct_saldo",
            F.round(
                F.col("saldo_total") * 100.0 /
                F.sum("saldo_total").over(ventana_global), 2))
        .orderBy(F.desc("saldo_total"))
    )
    return df_to_records(df)


# ══════════════════════════════════════════════════════════════
#  BLOQUE F — Trampas de Entrevista
# ══════════════════════════════════════════════════════════════

def f1_where_vs_having():
    clientes = leer_tabla("Clientes")
    cuentas  = leer_tabla("Cuentas")

    df = (
        clientes
        .join(cuentas, "id_cliente")
        .filter(F.col("saldo") > 20000)        # WHERE → antes del groupBy
        .groupBy("id_sucursal")
        .agg(F.count("*").alias("clientes_con_saldo_alto"))
        .orderBy(F.desc("clientes_con_saldo_alto"))
        .limit(15)
    )
    return df_to_records(df)


# ══════════════════════════════════════════════════════════════
#  BLOQUE G — Recomendación de Productos
# ══════════════════════════════════════════════════════════════

def g1_candidatos_deposito():
    clientes     = leer_tabla("Clientes")
    cuentas      = leer_tabla("Cuentas")
    score        = leer_tabla("ScoreCliente")
    cliente_prod = leer_tabla("ClienteProductos")
    productos    = leer_tabla("Productos")

    con_deposito = (
        cliente_prod.filter(F.col("estado") == "Activo")
        .join(productos.filter(F.col("categoria") == "Depósito"), "id_producto")
        .select("id_cliente")
        .distinct()
    )

    df = (
        clientes.filter(F.col("activo") == 1)
        .join(cuentas.filter(F.col("activa") == 1), "id_cliente")
        .join(score, "id_cliente")
        .filter(F.col("saldo") > 5000)
        .join(con_deposito, "id_cliente", how="left_anti")
        .withColumn("deposito_sugerido",
            F.when(F.col("saldo") >= 20000, "Depósito 24M — 3,80%")
             .when(F.col("saldo") >= 10000, "Depósito 12M — 3,20%")
             .otherwise("Depósito  6M — 2,50%"))
        .select(
            "id_cliente",
            F.concat(F.col("nombre"), F.lit(" "), F.col("apellidos")).alias("cliente"),
            "segmento",
            F.round("saldo", 2).alias("saldo"),
            "score_credito",
            "deposito_sugerido")
        .orderBy(F.desc("saldo"))
        .limit(25)
    )
    return df_to_records(df)


def g2_scoring_360():
    clientes     = leer_tabla("Clientes")
    cuentas      = leer_tabla("Cuentas")
    score        = leer_tabla("ScoreCliente")
    cliente_prod = leer_tabla("ClienteProductos")

    prod_activos = (
        cliente_prod.filter(F.col("estado") == "Activo")
        .groupBy("id_cliente")
        .agg(F.count("*").alias("productos_activos"))
    )

    df = (
        clientes.filter(F.col("activo") == 1)
        .join(cuentas.filter(F.col("activa") == 1), "id_cliente")
        .join(score, "id_cliente")
        .join(prod_activos, "id_cliente", how="left")
        .withColumn("edad",
            F.floor(F.datediff(F.current_date(), F.col("fecha_nacimiento")) / 365))
        # ── Cast BOOLEAN → INT antes de sumar ──────────────
        .withColumn("prop_ahorro",
            F.col("propension_ahorro").cast("int"))
        .withColumn("prop_inversion",
            F.col("propension_inversion").cast("int"))
        .withColumn("prop_seguro",
            F.col("propension_seguro").cast("int"))
        .withColumn("total_propensiones",
            F.col("prop_ahorro") +
            F.col("prop_inversion") +
            F.col("prop_seguro"))
        .filter(F.col("total_propensiones") >= 2)
        .withColumn("productos_activos",
            F.coalesce(F.col("productos_activos"), F.lit(0)))
        .select(
            "id_cliente",
            F.concat(F.col("nombre"), F.lit(" "), F.col("apellidos")).alias("cliente"),
            "segmento", "edad",
            F.round("saldo", 2).alias("saldo"),
            "score_credito", "riesgo",
            "total_propensiones", "productos_activos")
        .orderBy(F.desc("total_propensiones"), F.desc("saldo"))
        .limit(25)
    )
    return df_to_records(df)


# ══════════════════════════════════════════════════════════════
#  MAPA id → función
#  Flask y el orquestador usan este diccionario para llamar
#  a la función correcta por su id sin if/elif encadenados.
# ══════════════════════════════════════════════════════════════

MAPA = {
    "a1": a1_oferta_tarjeta,
    "a2": a2_scoring_multiproducto,
    "b1": b1_ranking_comparado,
    "b2": b2_top3_provincia,
    "b3": b3_lag_saldos,
    "c1": c1_sin_prestamo,
    "c2": c2_metricas_sucursales,
    "d1": d1_media_segmento,
    "e1": e1_cte_top_clientes,
    "e2": e2_distribucion_riesgo,
    "f1": f1_where_vs_having,
    "g1": g1_candidatos_deposito,
    "g2": g2_scoring_360,
}