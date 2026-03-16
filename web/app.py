# web/app.py
# =============================================================
#  Servidor Flask — punto de entrada de la aplicación web.
#  Arranca la SparkSession al iniciar y la reutiliza en cada
#  petición. Sirve el dashboard y las APIs on-demand.
# =============================================================

import sys
import os

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

from flask import Flask, jsonify, render_template, abort
from src.consultas.orquestador import ejecutar, ejecutar_todas
from src.consultas.catalogo import CONSULTAS, BLOQUES, CATALOGO_POR_ID
from config.conexion import get_spark

app = Flask(__name__)

# ── Arrancar SparkSession al iniciar Flask ───────────────────
# Se llama UNA vez aquí para que el singleton esté listo
# antes de que llegue la primera petición del usuario.
# Sin esto, el primer usuario esperaría 15-20 segundos.
print("\n🔥 Iniciando SparkSession...")
get_spark()
print("✅ SparkSession lista.\n")


# ── Ruta principal — Dashboard ───────────────────────────────
@app.route("/")
def index():
    """
    Sirve el dashboard principal.
    Pasa el catálogo completo al template para construir
    el menú lateral y las tarjetas de cada consulta.
    """
    return render_template(
        "index.html",
        consultas=CONSULTAS,
        bloques=BLOQUES
    )


# ── API: ejecutar una consulta por id ────────────────────────
@app.route("/api/<consulta_id>")
def api_consulta(consulta_id):
    """
    Ejecuta una consulta on-demand y devuelve JSON con:
    - metadata (titulo, bloque, icono, sql, pyspark, explicacion)
    - datos frescos de MariaDB procesados por PySpark
    - total_filas

    Ejemplo: GET /api/a1  →  oferta de tarjeta según saldo
    """
    if consulta_id not in CATALOGO_POR_ID:
        abort(404, description=f"Consulta '{consulta_id}' no encontrada.")

    try:
        resultado = ejecutar(consulta_id)
        return jsonify(resultado)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── API: catálogo completo (solo metadata, sin ejecutar) ─────
@app.route("/api/catalogo")
def api_catalogo():
    """
    Devuelve el catálogo completo sin ejecutar ninguna consulta.
    El dashboard lo usa al cargar para construir el menú
    y mostrar el SQL/PySpark de cada consulta antes de ejecutarla.
    """
    catalogo_limpio = [
        {
            "id":          c["id"],
            "titulo":      c["titulo"],
            "bloque":      c["bloque"],
            "icono":       c["icono"],
            "sql":         c["sql"],
            "pyspark":     c["pyspark"],
            "explicacion": c["explicacion"],
            "tablas":      c["tablas"],
        }
        for c in CONSULTAS
    ]
    return jsonify({
        "bloques":   BLOQUES,
        "consultas": catalogo_limpio
    })


# ── API: ejecutar todas las consultas ────────────────────────
@app.route("/api/todas")
def api_todas():
    """
    Ejecuta todas las consultas del catálogo en orden.
    Útil para precargar el dashboard completo de una vez.
    Puede tardar varios segundos — uso puntual, no en bucle.
    """
    try:
        resultados = ejecutar_todas()
        return jsonify(resultados)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── API: estado de la SparkSession ───────────────────────────
@app.route("/api/status")
def api_status():
    """
    Endpoint de salud — comprueba que Spark está activo.
    Docker y el balanceador de carga lo usan para health checks.
    """
    try:
        spark = get_spark()
        return jsonify({
            "status":  "ok",
            "spark":   spark.version,
            "app":     spark.sparkContext.appName,
            "tablas":  list(CATALOGO_POR_ID.keys()),
            "total":   len(CONSULTAS)
        })
    except Exception as e:
        return jsonify({"status": "error", "mensaje": str(e)}), 500


# ── Arranque ─────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(
        host="0.0.0.0",   # acepta conexiones externas (necesario en Docker)
        port=5001,
        debug=False        # False en producción — debug=True reinicia Spark en cada cambio
    )