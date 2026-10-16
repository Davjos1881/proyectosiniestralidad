"""DAG: ETL de siniestralidad vial de Cali -> MySQL (consumido por Power BI vía ODBC).

extraer -> [limpiar_accidentes, limpiar_lesionados] -> transformar -> cargar -> validar
            crear_bd (en paralelo con la limpieza; cargar espera a ambos)
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

IN_DIR = Path(os.environ.get("ETL_IN_DIR", "/opt/airflow/data/input"))
OUT_DIR = Path(os.environ.get("ETL_OUT_DIR", "/opt/airflow/data/output"))
STAGING = Path(os.environ.get("ETL_STAGING_DIR", "/opt/airflow/data/staging"))

F_ACC = IN_DIR / "siniestralidad_2016_2024.csv"
F_LES = IN_DIR / "lesionados-en-santiago-de-cali-del-2016-2025.csv"

COLUMNAS_ESPERADAS = {
    "accidentes": (F_ACC, {
        "Código", "Fecha", "Tipo confirmado", "Dirección reporte",
        "Tipo clase de accidente", "Tipo confirmado.1", "Medio de reporte",
        "Numero vehículos", "Tipo de vehículos implicados",
    }),
    "lesionados": (F_LES, {
        "Fecha", "Hora", "Dirección_Reporte", "Tipo_Confirmado.1", "Tipo_Confirmado",
        "Automovil", "Moto", "Ciclista", "Peaton", "Placa Agente de Transito",
    }),
}


@dag(
    dag_id="siniestralidad_cali_etl",
    description="CSV locales -> limpieza -> transformación -> MySQL",
    schedule=None,  # manual (MVP); cambiar a "@monthly" cuando haya CSV nuevos periódicos
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["siniestralidad", "cali", "etl"],
)
def siniestralidad_cali_etl():

    @task
    def extraer() -> dict:
        """Verifica que los CSV locales existan y tengan las columnas esperadas."""
        import pandas as pd

        STAGING.mkdir(parents=True, exist_ok=True)
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        for nombre, (ruta, cols) in COLUMNAS_ESPERADAS.items():
            if not ruta.exists():
                raise FileNotFoundError(f"Falta {ruta}. Copia el CSV a data/input/")
            header = {c.strip() for c in pd.read_csv(
                ruta, encoding="latin1", sep=";", nrows=0).columns}
            faltan = cols - header
            if faltan:
                raise ValueError(f"{nombre}: columnas faltantes {sorted(faltan)}")
            print(f"{nombre}: OK ({ruta.stat().st_size / 1e6:.1f} MB)")
        return {"accidentes": str(F_ACC), "lesionados": str(F_LES)}

    @task
    def limpiar_accidentes(rutas: dict) -> str:
        from etl import limpieza

        limpieza.REPORTE.clear()
        df = limpieza.limpiar_accidentes()
        (OUT_DIR / "reporte_limpieza_accidentes.txt").write_text(
            "\n".join(limpieza.REPORTE), encoding="utf-8")
        destino = STAGING / "siniestros_clean.parquet"
        df.to_parquet(destino, index=False)
        return str(destino)

    @task
    def limpiar_lesionados(rutas: dict) -> str:
        from etl import limpieza

        limpieza.REPORTE.clear()
        df = limpieza.limpiar_lesionados()
        (OUT_DIR / "reporte_limpieza_lesionados.txt").write_text(
            "\n".join(limpieza.REPORTE), encoding="utf-8")
        destino = STAGING / "lesionados_clean.parquet"
        df.to_parquet(destino, index=False)
        return str(destino)

    @task
    def transformar(ruta_acc: str, ruta_les: str) -> dict:
        from etl.transformacion import transformar as _transformar

        return _transformar(ruta_acc, ruta_les, STAGING)

    @task
    def crear_bd():
        from etl.carga import crear_bd as _crear_bd

        _crear_bd()

    @task
    def cargar(rutas_tablas: dict) -> dict:
        from etl.carga import cargar as _cargar

        return _cargar(rutas_tablas)

    @task
    def validar(cargadas: dict):
        from etl.carga import validar as _validar

        _validar(cargadas)

    rutas = extraer()
    acc = limpiar_accidentes(rutas)
    les = limpiar_lesionados(rutas)
    tablas = transformar(acc, les)

    bd = crear_bd()
    rutas >> bd

    carga = cargar(tablas)
    bd >> carga
    validar(carga)


siniestralidad_cali_etl()
