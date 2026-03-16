# src/consultas/catalogo.py
# =============================================================
#  Catálogo de consultas — metadatos, SQL y código PySpark
#  equivalente para cada consulta del proyecto bancario.
#  La lógica de ejecución está en los módulos ejecutores.
# =============================================================

CONSULTAS = [

    # ── BLOQUE A: CASE ──────────────────────────────────────
    {
        "id":      "a1",
        "titulo":  "A1 — Oferta de tarjeta según saldo",
        "bloque":  "CASE / Clasificación",
        "icono":   "💳",
        "tablas":  ["Clientes", "Cuentas"],
        "sql": """\
SELECT c.id_cuenta,
       CONCAT(cl.nombre,' ',cl.apellidos) AS cliente,
       c.tipo_cuenta,
       c.saldo,
       CASE
           WHEN c.saldo > 50000                  THEN 'Tarjeta Premium'
           WHEN c.saldo BETWEEN 10000 AND 50000  THEN 'Tarjeta Oro'
           ELSE                                       'Tarjeta Estándar'
       END AS oferta_recomendada
FROM Cuentas c
JOIN Clientes cl ON c.id_cliente = cl.id_cliente
WHERE c.activa = 1
LIMIT 25""",
        "pyspark": """\
cuentas.filter(F.col("activa") == 1)
.join(clientes, "id_cliente")
.withColumn("oferta_recomendada",
    F.when(F.col("saldo") > 50000,  "Tarjeta Premium")
     .when(F.col("saldo") >= 10000, "Tarjeta Oro")
     .otherwise("Tarjeta Estándar"))
.select("id_cuenta",
    F.concat("nombre", F.lit(" "), "apellidos").alias("cliente"),
    "tipo_cuenta",
    F.round("saldo", 2).alias("saldo"),
    "oferta_recomendada")
.limit(25)""",
        "explicacion": (
            "CASE WHEN evalúa condiciones de arriba a abajo y devuelve el primer "
            "resultado verdadero, como un if/else if. El orden importa: si pusieras "
            "primero saldo > 10000, los clientes con 60.000 € recibirían 'Tarjeta Oro' "
            "en lugar de 'Premium'. En PySpark: when().otherwise() encadenado."
        ),
    },

    {
        "id":      "a2",
        "titulo":  "A2 — Clasificación multiproducto (tarjeta + fondo + seguro)",
        "bloque":  "CASE / Clasificación",
        "icono":   "💳",
        "tablas":  ["Clientes", "Cuentas", "ScoreCliente"],
        "sql": """\
SELECT cl.id_cliente,
       CONCAT(cl.nombre,' ',cl.apellidos) AS cliente,
       cl.segmento,
       c.saldo,
       TIMESTAMPDIFF(YEAR, cl.fecha_nacimiento, CURDATE()) AS edad,
       CASE
           WHEN c.saldo > 50000 THEN 'Tarjeta Premium'
           WHEN c.saldo > 10000 THEN 'Tarjeta Oro'
           ELSE 'Tarjeta Estándar'
       END AS tarjeta,
       CASE
           WHEN sc.propension_inversion = 1 AND c.saldo > 5000 THEN 'Fondo Renta Variable'
           WHEN sc.propension_ahorro    = 1 AND c.saldo > 1000 THEN 'Fondo Renta Fija'
           ELSE '—'
       END AS fondo,
       CASE
           WHEN sc.propension_seguro = 1 THEN 'Seguro Vida Premium'
           ELSE 'Seguro Hogar'
       END AS seguro
FROM Clientes cl
JOIN Cuentas      c  ON cl.id_cliente = c.id_cliente AND c.activa = 1
JOIN ScoreCliente sc ON cl.id_cliente = sc.id_cliente
WHERE cl.activo = 1
LIMIT 25""",
        "pyspark": """\
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
    F.when((F.col("propension_inversion")==1) & (F.col("saldo")>5000),
            "Fondo Renta Variable")
     .when((F.col("propension_ahorro")==1) & (F.col("saldo")>1000),
            "Fondo Renta Fija")
     .otherwise("—"))
.withColumn("seguro",
    F.when(F.col("propension_seguro") == 1, "Seguro Vida Premium")
     .otherwise("Seguro Hogar"))
.orderBy(F.desc("saldo")).limit(25)""",
        "explicacion": (
            "Varios CASE independientes en el mismo SELECT recomiendan distintos productos "
            "en una sola pasada. En PySpark: un .withColumn() por cada CASE. "
            "Las condiciones AND en SQL se escriben con & en PySpark, siempre entre paréntesis. "
            "TIMESTAMPDIFF(YEAR,...) → F.floor(F.datediff(...) / 365)."
        ),
    },

    # ── BLOQUE B: WINDOW FUNCTIONS ───────────────────────────
    {
        "id":      "b1",
        "titulo":  "B1 — RANK vs ROW_NUMBER vs DENSE_RANK por sucursal",
        "bloque":  "Window Functions",
        "icono":   "💳",
        "tablas":  ["Clientes", "Cuentas"],
        "sql": """\
SELECT cl.id_sucursal,
       cl.id_cliente,
       CONCAT(cl.nombre,' ',cl.apellidos) AS cliente,
       c.saldo,
       RANK()       OVER(PARTITION BY cl.id_sucursal ORDER BY c.saldo DESC) AS rnk,
       DENSE_RANK() OVER(PARTITION BY cl.id_sucursal ORDER BY c.saldo DESC) AS dense_rnk,
       ROW_NUMBER() OVER(PARTITION BY cl.id_sucursal ORDER BY c.saldo DESC) AS row_num
FROM Clientes cl
JOIN Cuentas c ON cl.id_cliente = c.id_cliente
WHERE c.activa = 1
ORDER BY cl.id_sucursal, c.saldo DESC
LIMIT 30""",
        "pyspark": """\
ventana = Window.partitionBy("id_sucursal").orderBy(F.desc("saldo"))

clientes
.join(cuentas.filter(F.col("activa") == 1), "id_cliente")
.withColumn("rnk",       F.rank()      .over(ventana))
.withColumn("dense_rnk", F.dense_rank().over(ventana))
.withColumn("row_num",   F.row_number().over(ventana))
.select("id_sucursal", "id_cliente",
    F.concat("nombre", F.lit(" "), "apellidos").alias("cliente"),
    F.round("saldo", 2).alias("saldo"),
    "rnk", "dense_rnk", "row_num")
.orderBy("id_sucursal", F.desc("saldo")).limit(30)""",
        "explicacion": (
            "RANK: deja huecos en empates (1,1,3…). "
            "DENSE_RANK: sin huecos (1,1,2…). "
            "ROW_NUMBER: siempre único aunque haya empates (1,2,3…). "
            "En PySpark defines la ventana UNA vez con Window.partitionBy().orderBy() "
            "y la reutilizas en cada .withColumn(). "
            "PARTITION BY reinicia el contador por sucursal."
        ),
    },

    {
        "id":      "b2",
        "titulo":  "B2 — Top-3 clientes por saldo en cada provincia",
        "bloque":  "Window Functions",
        "icono":   "💳",
        "tablas":  ["Clientes", "Cuentas", "Provincias"],
        "sql": """\
SELECT * FROM (
    SELECT cl.id_cliente,
           CONCAT(cl.nombre,' ',cl.apellidos) AS cliente,
           p.nombre AS provincia,
           c.saldo,
           RANK() OVER(PARTITION BY cl.id_provincia ORDER BY c.saldo DESC) AS ranking
    FROM Clientes cl
    JOIN Cuentas    c ON cl.id_cliente   = c.id_cliente
    JOIN Provincias p ON cl.id_provincia = p.id_provincia
    WHERE c.activa = 1
) ranked
WHERE ranking <= 3
ORDER BY provincia, ranking
LIMIT 30""",
        "pyspark": """\
ventana = Window.partitionBy("id_provincia").orderBy(F.desc("saldo"))

clientes
.join(cuentas.filter(F.col("activa") == 1), "id_cliente")
.join(provincias, "id_provincia")
.withColumn("ranking", F.rank().over(ventana))
.filter(F.col("ranking") <= 3)
.select("id_cliente",
    F.concat("nombre", F.lit(" "), "apellidos").alias("cliente"),
    provincias["nombre"].alias("provincia"),
    F.round("saldo", 2).alias("saldo"),
    "ranking")
.orderBy("provincia", "ranking").limit(30)""",
        "explicacion": (
            "En SQL el filtro WHERE ranking<=3 necesita una subconsulta porque "
            "las window functions se calculan después del WHERE. "
            "En PySpark no existe este problema: simplemente añades .filter() "
            "después del .withColumn() y Spark gestiona el orden de ejecución."
        ),
    },

    {
        "id":      "b3",
        "titulo":  "B3 — Diferencia de saldo respecto al anterior (LAG)",
        "bloque":  "Window Functions",
        "icono":   "📉",
        "tablas":  ["Clientes", "Cuentas"],
        "sql": """\
SELECT cl.id_sucursal,
       CONCAT(cl.nombre,' ',cl.apellidos) AS cliente,
       c.saldo,
       LAG(c.saldo) OVER(PARTITION BY cl.id_sucursal ORDER BY c.saldo DESC) AS saldo_anterior,
       ROUND(
           c.saldo - LAG(c.saldo) OVER(PARTITION BY cl.id_sucursal ORDER BY c.saldo DESC)
       , 2) AS diferencia
FROM Clientes cl
JOIN Cuentas c ON cl.id_cliente = c.id_cliente
WHERE c.activa = 1
ORDER BY cl.id_sucursal, c.saldo DESC
LIMIT 25""",
        "pyspark": """\
ventana = Window.partitionBy("id_sucursal").orderBy(F.desc("saldo"))

clientes
.join(cuentas.filter(F.col("activa") == 1), "id_cliente")
.withColumn("saldo_anterior", F.lag("saldo").over(ventana))
.withColumn("diferencia",
    F.round(F.col("saldo") - F.col("saldo_anterior"), 2))
.select("id_sucursal",
    F.concat("nombre", F.lit(" "), "apellidos").alias("cliente"),
    F.round("saldo", 2).alias("saldo"),
    "saldo_anterior", "diferencia")
.orderBy("id_sucursal", F.desc("saldo")).limit(25)""",
        "explicacion": (
            "LAG(col) accede al valor de la fila anterior dentro de la partición. "
            "Su opuesto es LEAD(col) — fila siguiente. "
            "En PySpark: F.lag('col').over(ventana) y F.lead('col').over(ventana). "
            "La primera fila de cada partición devuelve None (NULL) porque no hay fila previa. "
            "Puedes pasar un valor por defecto: F.lag('saldo', 1, 0)."
        ),
    },

    # ── BLOQUE C: JOINs ─────────────────────────────────────
    {
        "id":      "c1",
        "titulo":  "C1 — Clientes SIN préstamo activo (LEFT JOIN + IS NULL)",
        "bloque":  "JOINs",
        "icono":   "💳",
        "tablas":  ["Clientes", "Cuentas", "Prestamos"],
        "sql": """\
SELECT cl.id_cliente,
       CONCAT(cl.nombre,' ',cl.apellidos) AS cliente,
       cl.segmento,
       c.saldo
FROM Clientes cl
JOIN Cuentas c ON cl.id_cliente = c.id_cliente AND c.activa = 1
LEFT JOIN Prestamos p ON cl.id_cliente = p.id_cliente AND p.estado = 'Activo'
WHERE p.id_prestamo IS NULL
  AND cl.activo = 1
ORDER BY c.saldo DESC
LIMIT 25""",
        "pyspark": """\
prestamos_activos = (
    prestamos.filter(F.col("estado") == "Activo")
    .select("id_cliente").distinct()
)

clientes.filter(F.col("activo") == 1)
.join(cuentas.filter(F.col("activa") == 1), "id_cliente")
.join(prestamos_activos, "id_cliente", how="left_anti")
.select("id_cliente",
    F.concat("nombre", F.lit(" "), "apellidos").alias("cliente"),
    "segmento",
    F.round("saldo", 2).alias("saldo"))
.orderBy(F.desc("saldo")).limit(25)""",
        "explicacion": (
            "LEFT JOIN + IS NULL detecta ausencias: incluye todos los clientes "
            "aunque no tengan préstamo y filtra los que no cruzaron. "
            "En PySpark el patrón equivalente es el left_anti join: "
            "más expresivo y sin el problema de NULLs que afecta a NOT IN."
        ),
    },

    {
        "id":      "c2",
        "titulo":  "C2 — Sucursales con métricas (JOIN múltiple + agregación)",
        "bloque":  "JOINs",
        "icono":   "💳",
        "tablas":  ["Sucursales", "Provincias", "Clientes", "Cuentas", "Prestamos"],
        "sql": """\
SELECT s.id_sucursal,
       s.nombre AS sucursal,
       pr.nombre AS provincia,
       COUNT(DISTINCT cl.id_cliente) AS num_clientes,
       ROUND(AVG(cu.saldo), 2)       AS saldo_medio,
       ROUND(SUM(cu.saldo), 2)       AS saldo_total,
       COUNT(DISTINCT p.id_prestamo) AS prestamos_activos
FROM Sucursales s
JOIN Provincias pr ON s.id_provincia    = pr.id_provincia
LEFT JOIN Clientes cl ON cl.id_sucursal = s.id_sucursal AND cl.activo = 1
LEFT JOIN Cuentas  cu ON cu.id_cliente  = cl.id_cliente AND cu.activa = 1
LEFT JOIN Prestamos p ON p.id_cliente   = cl.id_cliente AND p.estado  = 'Activo'
GROUP BY s.id_sucursal, s.nombre, pr.nombre
ORDER BY saldo_total DESC""",
        "pyspark": """\
sucursales
.join(provincias, "id_provincia")
.join(clientes.filter(F.col("activo")==1), "id_sucursal", how="left")
.join(cuentas.filter(F.col("activa")==1), "id_cliente",  how="left")
.join(prestamos.filter(F.col("estado")=="Activo")
               .select("id_cliente","id_prestamo"),
     "id_cliente", how="left")
.groupBy(sucursales["id_sucursal"],
         sucursales["nombre"].alias("sucursal"),
         provincias["nombre"].alias("provincia"))
.agg(
    F.countDistinct("id_cliente").alias("num_clientes"),
    F.round(F.avg("saldo"), 2).alias("saldo_medio"),
    F.round(F.sum("saldo"), 2).alias("saldo_total"),
    F.countDistinct("id_prestamo").alias("prestamos_activos"))
.orderBy(F.desc("saldo_total"))""",
        "explicacion": (
            "Con varios LEFT JOIN hay que usar COUNT(DISTINCT) para evitar duplicados: "
            "si un cliente tiene 2 cuentas y 1 préstamo, el JOIN genera 2 filas "
            "y un COUNT simple contaría ese cliente dos veces. "
            "En PySpark: F.countDistinct() — mismo concepto, misma solución."
        ),
    },

    # ── BLOQUE D: Subconsultas ───────────────────────────────
    {
        "id":      "d1",
        "titulo":  "D1 — Clientes con saldo superior a la media de su segmento",
        "bloque":  "Subconsultas",
        "icono":   "💳",
        "tablas":  ["Clientes", "Cuentas"],
        "sql": """\
SELECT cl.id_cliente,
       CONCAT(cl.nombre,' ',cl.apellidos) AS cliente,
       cl.segmento,
       ROUND(c.saldo, 2) AS saldo,
       ROUND((
           SELECT AVG(cu2.saldo)
           FROM Cuentas cu2
           JOIN Clientes cl2 ON cu2.id_cliente = cl2.id_cliente
           WHERE cl2.segmento = cl.segmento
       ), 2) AS media_segmento
FROM Clientes cl
JOIN Cuentas c ON cl.id_cliente = c.id_cliente
WHERE c.saldo > (
    SELECT AVG(cu2.saldo)
    FROM Cuentas cu2
    JOIN Clientes cl2 ON cu2.id_cliente = cl2.id_cliente
    WHERE cl2.segmento = cl.segmento
)
AND cl.activo = 1
ORDER BY cl.segmento, c.saldo DESC
LIMIT 25""",
        "pyspark": """\
# En PySpark las subconsultas correlacionadas se resuelven
# precalculando la media por segmento como DataFrame independiente
media_segmento = (
    clientes.join(cuentas, "id_cliente")
    .groupBy("segmento")
    .agg(F.round(F.avg("saldo"), 2).alias("media_segmento"))
)

clientes.filter(F.col("activo") == 1)
.join(cuentas, "id_cliente")
.join(media_segmento, "segmento")
.filter(F.col("saldo") > F.col("media_segmento"))
.select("id_cliente",
    F.concat("nombre", F.lit(" "), "apellidos").alias("cliente"),
    "segmento",
    F.round("saldo", 2).alias("saldo"),
    "media_segmento")
.orderBy("segmento", F.desc("saldo")).limit(25)""",
        "explicacion": (
            "Una subconsulta correlacionada en SQL se ejecuta una vez por cada fila — "
            "potente pero lenta con tablas grandes. "
            "En PySpark no existen subconsultas correlacionadas: "
            "se resuelven precalculando el agregado como DataFrame y luego haciendo join. "
            "Es más eficiente porque Spark calcula la media una sola vez para todos los segmentos."
        ),
    },

    # ── BLOQUE E: CTEs ───────────────────────────────────────
    {
        "id":      "e1",
        "titulo":  "E1 — CTE: clientes con alto saldo + su scoring",
        "bloque":  "CTEs",
        "icono":   "💳",
        "tablas":  ["Clientes", "Cuentas", "ScoreCliente"],
        "sql": """\
WITH saldo_total AS (
    SELECT id_cliente, ROUND(SUM(saldo), 2) AS total
    FROM Cuentas
    WHERE activa = 1
    GROUP BY id_cliente
),
top_clientes AS (
    SELECT cl.id_cliente,
           CONCAT(cl.nombre,' ',cl.apellidos) AS cliente,
           cl.segmento,
           st.total
    FROM Clientes cl
    JOIN saldo_total st ON cl.id_cliente = st.id_cliente
    WHERE st.total > 30000 AND cl.activo = 1
)
SELECT tc.*, sc.score_credito, sc.riesgo
FROM top_clientes tc
JOIN ScoreCliente sc ON tc.id_cliente = sc.id_cliente
ORDER BY tc.total DESC
LIMIT 25""",
        "pyspark": """\
# CTE 1 → DataFrame intermedio con nombre
saldo_total = (
    cuentas.filter(F.col("activa") == 1)
    .groupBy("id_cliente")
    .agg(F.round(F.sum("saldo"), 2).alias("total"))
)

# CTE 2 → otro DataFrame que usa el anterior
top_clientes = (
    clientes.filter(F.col("activo") == 1)
    .join(saldo_total, "id_cliente")
    .filter(F.col("total") > 30000)
    .select("id_cliente",
        F.concat("nombre", F.lit(" "), "apellidos").alias("cliente"),
        "segmento", "total")
)

# SELECT final
top_clientes.join(score, "id_cliente")
.select("id_cliente","cliente","segmento","total",
        "score_credito","riesgo")
.orderBy(F.desc("total")).limit(25)""",
        "explicacion": (
            "Las CTEs (WITH) se convierten en DataFrames intermedios nombrados. "
            "Cada CTE es simplemente un DataFrame que se reutiliza en el siguiente paso. "
            "Spark aplica lazy evaluation: no ejecuta nada hasta el .collect() final, "
            "optimizando todo el plan de ejecución junto — igual que el optimizador de SQL."
        ),
    },

    {
        "id":      "e2",
        "titulo":  "E2 — CTE: distribución de riesgo con % sobre total",
        "bloque":  "CTEs",
        "icono":   "💳",
        "tablas":  ["ScoreCliente", "Cuentas"],
        "sql": """\
WITH resumen AS (
    SELECT sc.riesgo,
           COUNT(*)               AS num_clientes,
           ROUND(AVG(c.saldo), 2) AS saldo_medio,
           ROUND(SUM(c.saldo), 2) AS saldo_total
    FROM ScoreCliente sc
    JOIN Cuentas c ON sc.id_cliente = c.id_cliente AND c.activa = 1
    GROUP BY sc.riesgo
)
SELECT riesgo, num_clientes, saldo_medio, saldo_total,
       ROUND(saldo_total * 100.0 / SUM(saldo_total) OVER(), 2) AS pct_saldo
FROM resumen
ORDER BY saldo_total DESC""",
        "pyspark": """\
resumen = (
    score.join(cuentas.filter(F.col("activa")==1), "id_cliente")
    .groupBy("riesgo")
    .agg(F.count("*").alias("num_clientes"),
         F.round(F.avg("saldo"),2).alias("saldo_medio"),
         F.round(F.sum("saldo"),2).alias("saldo_total"))
)

ventana_global = Window.rowsBetween(
    Window.unboundedPreceding, Window.unboundedFollowing
)

resumen.withColumn("pct_saldo",
    F.round(F.col("saldo_total") * 100.0 /
            F.sum("saldo_total").over(ventana_global), 2))
.orderBy(F.desc("saldo_total"))""",
        "explicacion": (
            "CTE + SUM() OVER() sin PARTITION calcula el total global. "
            "En PySpark: Window.rowsBetween(unboundedPreceding, unboundedFollowing) "
            "abarca todas las filas — equivale al OVER() vacío de SQL. "
            "Sin esta ventana necesitarías una subconsulta extra para obtener el total."
        ),
    },

    # ── BLOQUE F: Trampas de entrevista ──────────────────────
    {
        "id":      "f1",
        "titulo":  "F1 — WHERE vs HAVING (pregunta trampa)",
        "bloque":  "Trampas de Entrevista",
        "icono":   "⚠️",
        "tablas":  ["Clientes", "Cuentas"],
        "sql": """\
-- WHERE filtra FILAS antes de agrupar
SELECT cl.id_sucursal,
       COUNT(*) AS clientes_con_saldo_alto
FROM Clientes cl
JOIN Cuentas c ON cl.id_cliente = c.id_cliente
WHERE c.saldo > 20000
GROUP BY cl.id_sucursal
ORDER BY clientes_con_saldo_alto DESC
LIMIT 15""",
        "pyspark": """\
clientes
.join(cuentas, "id_cliente")
.filter(F.col("saldo") > 20000)       # WHERE → .filter() antes del groupBy
.groupBy("id_sucursal")
.agg(F.count("*").alias("clientes_con_saldo_alto"))
.orderBy(F.desc("clientes_con_saldo_alto")).limit(15)

# Si el filtro fuera sobre el agregado (HAVING):
# .groupBy("id_sucursal")
# .agg(F.count("*").alias("total"))
# .filter(F.col("total") > 5)         # HAVING → .filter() después del groupBy""",
        "explicacion": (
            "WHERE actúa antes del GROUP BY: filtra filas individuales. "
            "HAVING actúa después: filtra sobre el resultado de la agregación. "
            "En PySpark no existe HAVING como cláusula — simplemente usas .filter() "
            "antes del .groupBy() para WHERE, o después para HAVING. "
            "La posición del .filter() en la cadena es lo que marca la diferencia."
        ),
    },

    # ── BLOQUE G: Recomendación de productos ─────────────────
    {
        "id":      "g1",
        "titulo":  "G1 — Clientes candidatos a depósito (sin producto activo)",
        "bloque":  "Recomendación de Productos",
        "icono":   "💳",
        "tablas":  ["Clientes", "Cuentas", "ScoreCliente", "ClienteProductos", "Productos"],
        "sql": """\
SELECT cl.id_cliente,
       CONCAT(cl.nombre,' ',cl.apellidos) AS cliente,
       cl.segmento,
       ROUND(c.saldo, 2) AS saldo,
       sc.score_credito,
       CASE
           WHEN c.saldo >= 20000 THEN 'Depósito 24M — 3,80%'
           WHEN c.saldo >= 10000 THEN 'Depósito 12M — 3,20%'
           ELSE                       'Depósito  6M — 2,50%'
       END AS deposito_sugerido
FROM Clientes cl
JOIN Cuentas      c  ON cl.id_cliente = c.id_cliente  AND c.activa = 1
JOIN ScoreCliente sc ON cl.id_cliente = sc.id_cliente
WHERE c.saldo > 5000
  AND cl.activo = 1
  AND cl.id_cliente NOT IN (
      SELECT cp.id_cliente
      FROM ClienteProductos cp
      JOIN Productos p ON cp.id_producto = p.id_producto
      WHERE p.categoria = 'Depósito' AND cp.estado = 'Activo'
  )
ORDER BY c.saldo DESC
LIMIT 25""",
        "pyspark": """\
con_deposito = (
    cliente_prod.filter(F.col("estado") == "Activo")
    .join(productos.filter(F.col("categoria") == "Depósito"), "id_producto")
    .select("id_cliente").distinct()
)

clientes.filter(F.col("activo") == 1)
.join(cuentas.filter(F.col("activa") == 1), "id_cliente")
.join(score, "id_cliente")
.filter(F.col("saldo") > 5000)
.join(con_deposito, "id_cliente", how="left_anti")
.withColumn("deposito_sugerido",
    F.when(F.col("saldo") >= 20000, "Depósito 24M — 3,80%")
     .when(F.col("saldo") >= 10000, "Depósito 12M — 3,20%")
     .otherwise("Depósito 6M — 2,50%"))
.select("id_cliente",
    F.concat("nombre", F.lit(" "), "apellidos").alias("cliente"),
    "segmento", F.round("saldo",2).alias("saldo"),
    "score_credito", "deposito_sugerido")
.orderBy(F.desc("saldo")).limit(25)""",
        "explicacion": (
            "NOT IN (subconsulta) → left_anti join en PySpark. "
            "Construyes el conjunto a excluir como DataFrame y haces join left_anti. "
            "Es más eficiente que NOT IN en SQL y no tiene el problema de NULLs. "
            "Patrón típico de cross-selling: alto saldo + sin producto = oportunidad."
        ),
    },

    {
        "id":      "g2",
        "titulo":  "G2 — Scoring 360°: propensión múltiple + productos activos",
        "bloque":  "Recomendación de Productos",
        "icono":   "🌐",
        "tablas":  ["Clientes", "Cuentas", "ScoreCliente", "ClienteProductos"],
        "sql": """\
SELECT cl.id_cliente,
       CONCAT(cl.nombre,' ',cl.apellidos) AS cliente,
       cl.segmento,
       TIMESTAMPDIFF(YEAR, cl.fecha_nacimiento, CURDATE()) AS edad,
       ROUND(c.saldo, 2) AS saldo,
       sc.score_credito,
       sc.riesgo,
       (sc.propension_ahorro + sc.propension_inversion + sc.propension_seguro)
           AS total_propensiones,
       COUNT(cp.id_contrato) AS productos_activos
FROM Clientes cl
JOIN Cuentas         c  ON cl.id_cliente = c.id_cliente  AND c.activa = 1
JOIN ScoreCliente    sc ON cl.id_cliente = sc.id_cliente
LEFT JOIN ClienteProductos cp ON cl.id_cliente = cp.id_cliente AND cp.estado = 'Activo'
WHERE cl.activo = 1
GROUP BY cl.id_cliente, cl.nombre, cl.apellidos, cl.segmento, cl.fecha_nacimiento,
         c.saldo, sc.score_credito, sc.riesgo,
         sc.propension_ahorro, sc.propension_inversion, sc.propension_seguro
HAVING total_propensiones >= 2
ORDER BY total_propensiones DESC, c.saldo DESC
LIMIT 25""",
        "pyspark": """\
        score = score
    .withColumn("prop_ahorro",    F.col("propension_ahorro").cast("int"))
    .withColumn("prop_inversion", F.col("propension_inversion").cast("int"))
    .withColumn("prop_seguro",    F.col("propension_seguro").cast("int"))
    
prod_activos = (
    cliente_prod.filter(F.col("estado") == "Activo")
    .groupBy("id_cliente")
    .agg(F.count("*").alias("productos_activos"))
)

clientes.filter(F.col("activo") == 1)
.join(cuentas.filter(F.col("activa") == 1), "id_cliente")
.join(score, "id_cliente")
.join(prod_activos, "id_cliente", how="left")
.withColumn("edad",
    F.floor(F.datediff(F.current_date(), F.col("fecha_nacimiento")) / 365))
.withColumn("total_propensiones",
    F.col("prop_ahorro") +
    F.col("prop_inversion") +
    F.col("prop_seguro"))
.filter(F.col("total_propensiones") >= 2)
.withColumn("productos_activos",
    F.coalesce(F.col("productos_activos"), F.lit(0)))
.orderBy(F.desc("total_propensiones"), F.desc("saldo")).limit(25)""",
        "explicacion": (
            "HAVING sobre alias calculado en SELECT → .filter() después del .withColumn(). "
            "En PySpark no existe HAVING: filtras el DataFrame después de calcular la columna. "
            "LEFT JOIN + GROUP BY → pre-agregar productos como DataFrame separado y join left. "
            "F.coalesce() maneja los NULL del LEFT JOIN igual que COALESCE en SQL."
        ),
    },
]

# Índice rápido por id para búsquedas O(1)
CATALOGO_POR_ID = {c["id"]: c for c in CONSULTAS}

# Bloques únicos en orden de aparición
BLOQUES = list(dict.fromkeys(c["bloque"] for c in CONSULTAS))