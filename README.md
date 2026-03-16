# PySpark SQL Bancario

Proyecto de análisis y recomendación bancaria que demuestra la equivalencia entre **SQL** y **PySpark DataFrame API**, usando datos reales de una base de datos MariaDB.

Desarrollado como proyecto técnico de entrevista para demostrar conocimientos en SQL avanzado y PySpark en un contexto de banca minorista.

---

## Stack técnico

| Capa | Tecnología |
|---|---|
| Procesamiento | Apache Spark 3.5.1 (PySpark) |
| Base de datos | MariaDB / MySQL via JDBC |
| Backend | Flask 3.x |
| Frontend | HTML + CSS + Vanilla JS |
| Driver JDBC | mysql-connector-j 8.4.0 |
| Runtime | Python 3.12 |

---

## Arquitectura

```
Navegador
    │  GET /api/<id>
    ▼
Flask (web/app.py)
    │  ejecutar(id)
    ▼
Orquestador (src/consultas/orquestador.py)
    ├── metadata  ◄── Catálogo (src/consultas/catalogo.py)
    └── datos     ◄── Ejecutores (src/consultas/ejecutores.py)
                            │  leer_tabla()
                            ▼
                    SparkSession singleton (config/conexion.py)
                            │  JDBC
                            ▼
                        MariaDB
```

**Patrón on-demand**: SparkSession arranca una sola vez con Flask y se reutiliza en cada petición. Los datos se leen frescos de MariaDB en el momento de cada consulta — sin ficheros intermedios, sin caché.

---

## Consultas implementadas

| ID | Consulta | Concepto SQL | Concepto PySpark |
|---|---|---|---|
| A1 | Oferta de tarjeta según saldo | `CASE WHEN` | `when().otherwise()` |
| A2 | Clasificación multiproducto | `CASE` múltiple | `.withColumn()` encadenado |
| B1 | RANK vs DENSE_RANK vs ROW_NUMBER | Window Functions | `Window.partitionBy()` |
| B2 | Top-3 clientes por provincia | `RANK + subconsulta` | `.filter()` post window |
| B3 | Diferencia de saldo (LAG) | `LAG()` | `F.lag().over()` |
| C1 | Clientes sin préstamo activo | `LEFT JOIN + IS NULL` | `left_anti` join |
| C2 | Métricas por sucursal | `JOIN múltiple + GROUP BY` | `groupBy().agg()` |
| D1 | Saldo superior a media del segmento | Subconsulta correlacionada | DataFrame precalculado |
| E1 | Top clientes + scoring (CTE) | `WITH` encadenado | DataFrames intermedios |
| E2 | Distribución de riesgo con % | `CTE + SUM() OVER()` | `Window.rowsBetween()` |
| F1 | WHERE vs HAVING | Pregunta trampa | `.filter()` antes/después de `groupBy()` |
| G1 | Candidatos a depósito | `NOT IN (subconsulta)` | `left_anti` join |
| G2 | Scoring 360° | `HAVING` sobre alias | `.filter()` post `.withColumn()` |

---

## Estructura del proyecto

```
pyspark-scl/
├── config/
│   ├── conexion.py            # SparkSession singleton + JDBC (excluido de git)
│   └── conexion.example.py   # Plantilla de configuración
├── src/
│   └── consultas/
│       ├── __init__.py
│       ├── catalogo.py        # Metadatos, SQL y código PySpark de cada consulta
│       ├── ejecutores.py      # Funciones PySpark — lógica de transformación
│       └── orquestador.py     # Une catálogo + ejecutores, lo llama Flask
├── web/
│   ├── app.py                 # Servidor Flask — rutas API y dashboard
│   └── templates/
│       └── index.html         # Dashboard con tabs SQL/PySpark y resultados
├── spark-jars/                # Driver JDBC (excluido de git, ver instalación)
├── test_conexion.py           # Test de conectividad con MariaDB
├── requirements.txt
└── README.md
```

---

## Instalación

### 1. Clonar el repositorio

```bash
git clone https://github.com/tu-usuario/pyspark-scl.git
cd pyspark-scl
```

### 2. Crear entorno virtual e instalar dependencias

```bash
python -m venv venv
source venv/bin/activate       # Linux/Mac
# venv\Scripts\activate        # Windows

pip install -r requirements.txt
```

### 3. Configurar la conexión a la base de datos

```bash
cp config/conexion.example.py config/conexion.py
```

Edita `config/conexion.py` con tus credenciales:

```python
JDBC_URL = "jdbc:mysql://TU_HOST:3306/TU_BASE_DE_DATOS?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"

JDBC_PROPS = {
    "user":     "TU_USUARIO",
    "password": "TU_PASSWORD",
    "driver":   "com.mysql.cj.jdbc.Driver"
}
```

### 4. Descargar el driver JDBC de MySQL

```bash
mkdir -p spark-jars
wget https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar \
     -O spark-jars/mysql-connector-j-8.4.0.jar
```

### 5. Verificar la conexión

```bash
python test_conexion.py
```

Salida esperada:

```
✅ Clientes             — 1500 filas
✅ Cuentas              — 2100 filas
✅ Prestamos            —  800 filas
✅ ScoreCliente         — 1500 filas
✅ Sucursales           —   15 filas
✅ Provincias           —   20 filas
```

---

## Arranque

```bash
python web/app.py
```

Abre el navegador en `http://localhost:5000`.

En macOS, si el puerto 5000 está ocupado por AirPlay Receiver:

```
Sistema → Configuración → General → AirDrop y Handoff → Receptor AirPlay → Desactivar
```

O cambia el puerto en `web/app.py`:

```python
app.run(host="0.0.0.0", port=5001)
```

---

## Uso del dashboard

El dashboard presenta cada consulta en una tarjeta con tres secciones:

- **Tab SQL** — consulta SQL original con syntax highlight
- **Tab PySpark** — código PySpark equivalente con syntax highlight
- **Explicación** — diferencias conceptuales entre ambas implementaciones
- **▶ Ejecutar** — lanza la consulta on-demand contra MariaDB via PySpark y muestra los resultados en tabla

El botón **"Ejecutar todas"** lanza las 13 consultas en paralelo.

### Endpoints API

| Endpoint | Descripción |
|---|---|
| `GET /` | Dashboard web |
| `GET /api/<id>` | Ejecuta una consulta y devuelve JSON con metadata + datos |
| `GET /api/catalogo` | Devuelve metadata de todas las consultas sin ejecutar |
| `GET /api/todas` | Ejecuta todas las consultas |
| `GET /api/status` | Health check — versión de Spark y estado de la sesión |

---

## Conceptos clave demostrados

### SQL → PySpark: tabla de equivalencias

| SQL | PySpark |
|---|---|
| `CASE WHEN ... THEN ... ELSE ... END` | `F.when().when().otherwise()` |
| `LEFT JOIN ... WHERE x IS NULL` | `.join(df, "col", how="left_anti")` |
| `NOT IN (subconsulta)` | `.join(df, "col", how="left_anti")` |
| `PARTITION BY ... ORDER BY` | `Window.partitionBy().orderBy()` |
| `LAG(col) OVER(...)` | `F.lag("col").over(ventana)` |
| `SUM() OVER()` sin PARTITION | `Window.rowsBetween(unboundedPreceding, unboundedFollowing)` |
| `WITH cte AS (...)` | DataFrame intermedio nombrado |
| `HAVING` sobre agregado | `.filter()` después de `.groupBy().agg()` |
| `COUNT(DISTINCT col)` | `F.countDistinct("col")` |
| `TIMESTAMPDIFF(YEAR, ...)` | `F.floor(F.datediff(...) / 365)` |

### Patrón singleton de SparkSession

```python
_spark = None

def get_spark() -> SparkSession:
    global _spark
    if _spark is None:
        _spark = SparkSession.builder.appName("...").getOrCreate()
    return _spark
```

SparkSession arranca una sola vez cuando Flask inicia y se reutiliza en todas las peticiones. Crear múltiples sesiones en el mismo proceso es un error común en producción.

### Por qué `left_anti` en lugar de `NOT IN`

`NOT IN (subconsulta)` falla silenciosamente si la subconsulta devuelve algún `NULL`. El `left_anti` join es el patrón correcto en PySpark: más explícito, más eficiente y sin el problema de NULLs.

```python
# SQL: WHERE id NOT IN (SELECT id FROM tabla WHERE ...)
excluir = otra_tabla.filter(...).select("id").distinct()
df.join(excluir, "id", how="left_anti")
```

---

## Requisitos del sistema

- Python 3.10+
- Java 11 (requerido por Apache Spark)
- 4 GB RAM mínimo (Spark reserva ~1 GB por defecto)
- Acceso a MariaDB/MySQL en red

Verificar Java:

```bash
java -version
# openjdk version "11.x.x"
```

---

## Próximos pasos

- [ ] Dockerizar con `docker-compose` (contenedor Spark + contenedor Flask)
- [ ] Despliegue en VPS con Nginx como reverse proxy
- [ ] Añadir batch programado con APScheduler o Airflow
- [ ] Tests unitarios con `pytest` para cada ejecutor
- [ ] Añadir gráficos Chart.js para consultas de distribución (E2, C2)

---

## Autor

Proyecto desarrollado como demostración técnica de SQL avanzado y PySpark  
para proceso de selección en banca de datos — 2026
