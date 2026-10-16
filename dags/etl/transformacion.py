"""Transformación: dimensión de fechas, tablas de hechos listas para MySQL y agregados."""
from pathlib import Path

import pandas as pd

from etl.limpieza import DIAS, FESTIVOS

MESES = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio",
         "agosto", "septiembre", "octubre", "noviembre", "diciembre"]
MAX_STR = 500  # longitud máxima de las columnas VARCHAR de direcciones/textos


def _recortar(df, excepto=()):
    """Evita errores de 'Data too long' en MySQL."""
    for c in df.select_dtypes(include=["object", "string"]).columns:
        if c not in excepto:
            df[c] = df[c].astype("string").str.slice(0, MAX_STR).astype(object)
    return df


def construir_dim_fecha(f_min, f_max):
    dias = pd.date_range(f"{f_min.year}-01-01", f"{f_max.year}-12-31", freq="D")
    d = pd.DataFrame({"fecha": dias})
    d["anio"] = d["fecha"].dt.year
    d["mes"] = d["fecha"].dt.month
    d["nombre_mes"] = d["mes"].map(lambda m: MESES[m - 1])
    d["trimestre"] = d["fecha"].dt.quarter
    d["anio_mes"] = d["fecha"].dt.strftime("%Y-%m")
    d["dia"] = d["fecha"].dt.day
    d["dia_semana_num"] = d["fecha"].dt.dayofweek
    d["dia_semana"] = d["dia_semana_num"].map(dict(enumerate(DIAS)))
    d["es_fin_semana"] = d["dia_semana_num"] >= 5
    fechas = d["fecha"].dt.date
    d["es_festivo"] = fechas.map(lambda x: x in FESTIVOS)
    d["nombre_festivo"] = fechas.map(FESTIVOS.get)
    d["fecha"] = fechas
    return d


def transformar(ruta_acc, ruta_les, staging):
    staging = Path(staging)
    acc = pd.read_parquet(ruta_acc)
    les = pd.read_parquet(ruta_les)

    acc = _recortar(acc, excepto=("vehiculos_implicados",))
    les = _recortar(les)
    acc["num_vehiculos"] = acc["num_vehiculos"].round().astype("Int64")

    dim_fecha = construir_dim_fecha(
        min(acc["fecha"].min(), les["fecha"].min()),
        max(acc["fecha"].max(), les["fecha"].max()),
    )

    # Agregados (se calculan antes de convertir 'fecha' a date)
    agg_sin = (
        acc.assign(clase_accidente=acc["clase_accidente"].fillna("Sin dato"))
        .groupby(["anio", "mes", "severidad", "clase_accidente"])
        .size().reset_index(name="n_siniestros")
    )
    agg_les = (
        les.dropna(subset=["hora"])
        .groupby(["anio", "dia_semana_num", "dia_semana", "hora"])
        .size().reset_index(name="n_lesionados")
    )
    agg_les["hora"] = agg_les["hora"].astype(int)

    acc["fecha"] = acc["fecha"].dt.date
    les["fecha"] = les["fecha"].dt.date

    tablas = {
        "dim_fecha": dim_fecha,
        "fact_siniestros": acc,
        "fact_lesionados": les,
        "agg_siniestros_mensual": agg_sin,
        "agg_lesionados_hora": agg_les,
    }
    rutas = {}
    for nombre, df in tablas.items():
        ruta = staging / f"t_{nombre}.parquet"
        df.to_parquet(ruta, index=False)
        rutas[nombre] = str(ruta)
        print(f"{nombre}: {len(df):,} filas")
    return rutas
