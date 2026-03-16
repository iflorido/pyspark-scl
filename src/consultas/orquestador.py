# src/consultas/orquestador.py
import sys
import os

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT_DIR)

from src.consultas.catalogo import CATALOGO_POR_ID
from src.consultas import ejecutores


def ejecutar(consulta_id: str) -> dict:
    """
    Busca la consulta en el catálogo, la ejecuta y devuelve
    metadatos + datos frescos de MariaDB listos para el dashboard.
    """
    # O(1) con el índice en lugar de recorrer la lista
    meta = CATALOGO_POR_ID.get(consulta_id)

    if meta is None:
        raise ValueError(f"Consulta '{consulta_id}' no existe en el catálogo.")

    # Llama a la función PySpark correspondiente
    datos = ejecutores.MAPA[consulta_id]()

    return {
        "id":          meta["id"],
        "titulo":      meta["titulo"],
        "bloque":      meta["bloque"],
        "icono":       meta["icono"],
        "sql":         meta["sql"],
        "pyspark":     meta["pyspark"],
        "explicacion": meta["explicacion"],
        "tablas":      meta["tablas"],
        "datos":       datos,
        "total_filas": len(datos),
    }


def ejecutar_bloque(bloque: str) -> list:
    """
    Ejecuta todas las consultas de un bloque (ej: 'Window Functions')
    y devuelve una lista de resultados.
    """
    return [
        ejecutar(c["id"])
        for c in CATALOGO_POR_ID.values()
        if c["bloque"] == bloque
    ]


def ejecutar_todas() -> list:
    """
    Ejecuta todas las consultas del catálogo en orden.
    Flask lo usa cuando el usuario pulsa 'Actualizar todo'.
    """
    return [ejecutar(c["id"]) for c in CATALOGO_POR_ID.values()]
