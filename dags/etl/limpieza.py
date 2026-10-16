"""
ETL de limpieza - Siniestralidad vial Cali
Entradas : siniestralidad_2016_2024.csv, lesionados-en-santiago-de-cali-del-2016-2025.csv
Salidas  : siniestralidad_clean.csv, lesionados_clean.csv

Uso:  python etl_limpieza.py <carpeta_entrada> <carpeta_salida>
"""
import os
import re
import unicodedata
from pathlib import Path

import holidays
import numpy as np
import pandas as pd

IN_DIR = Path(os.environ.get("ETL_IN_DIR", "."))
OUT_DIR = Path(os.environ.get("ETL_OUT_DIR", "."))
OUT_DIR.mkdir(parents=True, exist_ok=True)

F_ACC = IN_DIR / "siniestralidad_2016_2024.csv"
F_LES = IN_DIR / "lesionados-en-santiago-de-cali-del-2016-2025.csv"

DIAS = ["lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo"]
FESTIVOS = holidays.Colombia(years=range(2016, 2027))
REPORTE = []  # líneas del reporte de calidad


def log(msg=""):
    print(msg)
    REPORTE.append(msg)


# ----------------------------------------------------------------- utilidades
def key(s):
    """Clave canónica: minúsculas, sin tildes, sin signos ni '?' corrupto."""
    if pd.isna(s):
        return None
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    return re.sub(r"[^a-z0-9]", "", s)


def map_cat(serie, mapping, otro=None):
    """Mapea una serie por clave canónica; lo no mapeado queda como `otro` (o se conserva)."""
    k = serie.map(key)
    out = k.map(mapping)
    if otro is not None:
        out = out.where(out.notna() | serie.isna(), otro)
    return out


def leer(path):
    df = pd.read_csv(path, encoding="latin1", sep=";", dtype=str, keep_default_na=True)
    df.columns = [c.strip() for c in df.columns]
    df = df.apply(lambda c: c.str.strip())
    return df.replace({".": pd.NA, "": pd.NA})


ABREV = [
    (r"\b(CRA|KR|CARR|CRR|CR)\b\.?", "CARRERA"),
    (r"\b(CLL|CLLE|CL)\b\.?", "CALLE"),
    (r"\b(AV|AVDA|AVD)\b\.?", "AVENIDA"),
    (r"\bDIAG\b\.?", "DIAGONAL"),
    (r"\b(TRANSV|TV|TRV)\b\.?", "TRANSVERSAL"),
]


def norm_direccion(s):
    if pd.isna(s):
        return pd.NA
    t = re.sub(r"\s+", " ", str(s).upper()).strip(" .,-")
    for pat, rep in ABREV:
        t = re.sub(pat, rep, t)
    return t if t else pd.NA


def dir_valida(s):
    """Dirección usable para geocodificar: no vacía y con al menos un número."""
    return s.notna() & s.fillna("").str.contains(r"\d")


def features_fecha(df):
    df["anio"] = df["fecha"].dt.year
    df["mes"] = df["fecha"].dt.month
    df["dia_semana_num"] = df["fecha"].dt.dayofweek
    df["dia_semana"] = df["dia_semana_num"].map(dict(enumerate(DIAS)))
    df["es_fin_semana"] = df["dia_semana_num"] >= 5
    df["es_festivo"] = df["fecha"].dt.date.map(lambda d: d in FESTIVOS)
    return df


# ------------------------------------------------------- catálogos de mapeo
SEVERIDAD = {
    "solodaos": "Solo daños", "solodanos": "Solo daños",
    "conlesionado": "Con lesionado",
    "confallecido": "Con fallecido", "confallecidoforaneo": "Con fallecido",
}
CLASE = {
    "choque": "Choque", "atropello": "Atropello", "volcamiento": "Volcamiento",
    "caidadeocupante": "Caída de ocupante", "cadaocupante": "Caída de ocupante",
    "otro": "Otro", "noaplica": "No aplica", "daos": "Daños", "danos": "Daños",
    "incendio": "Incendio",
}
DETALLE = {
    "negativo": "Negativo", "solodaos": "Solo daños", "solodanos": "Solo daños",
    "arregloformal": "Arreglo formal",
    "choqueconlesionados": "Choque con lesionados", "conheridos": "Con heridos",
    "noquiereintervencin": "No quiere intervención",
    "noquiereintervencion": "No quiere intervención",
    "poratropello": "Atropello", "porvolcamiento": "Volcamiento",
    "porcaidadeocupante": "Caída de ocupante",
    "falsaalarma": "Falsa alarma", "positivo": "Positivo",
    "inspeccionacadaver": "Inspección a cadáver", "muertoenlavia": "Muerto en la vía",
    "conmuertos": "Con muertos", "procedimiento": "Procedimiento",
    "embriaguez": "Embriaguez",
    "lesioneshumanogolpeavehiculoestacionado": "Lesiones (humano golpea vehículo estacionado)",
}
MEDIO = {
    "telefnico127": "Telefónico 127", "telefonico127": "Telefónico 127",
    "cad123": "CAD 123", "radio": "Radio", "cat127": "CAT 127",
    "agenteradio": "Agente radio", "agentecad123": "Agente CAD 123",
    "redessociales": "Redes sociales", "agentetracr": "Agente TRACR",
    "movilidadcali": "Movilidad Cali", "otrovalor": "Otro",
}
VEHICULOS = {
    "veh_moto": r"MOTOCICLETA|MOTOCICLO",
    "veh_motocarro": r"MOTOCARRO",
    "veh_automovil": r"AUTOMOVIL",
    "veh_camioneta_campero": r"CAMIONETA|CAMPERO|\bVAN\b",
    "veh_bus": r"\bBUS|BUSETA|MICROBUS",
    "veh_carga_pesada": r"CAMION|VOLQUETA|TRACTO",
    "veh_bicicleta": r"BICICLETA",
}


# ===================================================================== ACCIDENTES
def limpiar_accidentes():
    log("=" * 70)
    log("DATASET 1: siniestralidad_2016_2024.csv")
    log("=" * 70)
    df = leer(F_ACC)
    n0 = len(df)
    log(f"Filas originales: {n0:,}")

    df = df.rename(columns={
        "Código": "codigo", "Fecha": "fecha_txt", "Tipo confirmado": "severidad_raw",
        "Dirección reporte": "direccion_original",
        "Tipo clase de accidente": "clase_accidente_raw",
        "Tipo confirmado.1": "detalle_raw", "Medio de reporte": "medio_raw",
        "Numero vehículos": "num_vehiculos_raw",
        "Tipo de vehículos implicados": "vehiculos_implicados",
    }).drop(columns=["Tipo clase de accidente.1"])  # duplicada al 99.9997%

    # filas sin código/fecha/severidad
    antes = len(df)
    df = df.dropna(subset=["codigo", "fecha_txt", "severidad_raw"])
    log(f"- Filas sin código/fecha/severidad eliminadas: {antes - len(df)}")

    # duplicados exactos
    antes = len(df)
    df = df.drop_duplicates()
    log(f"- Duplicados exactos eliminados: {antes - len(df)}")

    # fecha
    df["fecha"] = pd.to_datetime(df["fecha_txt"], format="%d/%m/%Y", errors="coerce")
    fallo = df["fecha"].isna().sum()
    log(f"- Fechas no parseables: {fallo}")
    df = df.dropna(subset=["fecha"])

    # severidad: se descartan los NEGATIVOS (no son accidentes confirmados)
    es_neg = df["severidad_raw"].map(key) == "negativo"
    log(f"- Registros 'Negativo' descartados: {es_neg.sum():,} ({es_neg.mean():.1%})")
    df = df[~es_neg].copy()
    df["severidad"] = map_cat(df["severidad_raw"], SEVERIDAD)
    sin_map = df["severidad"].isna().sum()
    log(f"- Severidad sin mapear (descartadas): {sin_map}")
    df = df.dropna(subset=["severidad"])
    df["fallecido_foraneo"] = df["severidad_raw"].map(key) == "confallecidoforaneo"

    # categorías
    df["clase_accidente"] = map_cat(df["clase_accidente_raw"], CLASE, otro="Otro")
    df["detalle_atencion"] = map_cat(df["detalle_raw"], DETALLE, otro="Otro")
    medio = map_cat(df["medio_raw"], MEDIO)
    largo = df["medio_raw"].fillna("").str.len() > 40  # relatos libres
    df["medio_reporte"] = medio.where(~largo, "Otro (texto libre)")
    df["medio_reporte"] = df["medio_reporte"].where(
        df["medio_reporte"].notna() | df["medio_raw"].isna(), "Otro")

    # numéricos y vehículos
    df["num_vehiculos"] = pd.to_numeric(
        df["num_vehiculos_raw"].str.replace(r"\.$", "", regex=True), errors="coerce")
    veh = df["vehiculos_implicados"].fillna("").str.upper()
    sin_veh = df["vehiculos_implicados"].isna()
    for col, pat in VEHICULOS.items():
        df[col] = veh.str.contains(pat, regex=True).astype("boolean").mask(sin_veh)

    # direcciones
    df["direccion_norm"] = df["direccion_original"].map(norm_direccion)
    df["direccion_valida"] = dir_valida(df["direccion_norm"])
    df["direccion_con_corrupcion"] = df["direccion_original"].fillna("").str.contains(r"\?")

    df = features_fecha(df)

    cols = [
        "codigo", "fecha", "anio", "mes", "dia_semana_num", "dia_semana",
        "es_fin_semana", "es_festivo", "severidad", "fallecido_foraneo",
        "clase_accidente", "detalle_atencion", "medio_reporte", "num_vehiculos",
        "vehiculos_implicados", *VEHICULOS, "direccion_original", "direccion_norm",
        "direccion_valida", "direccion_con_corrupcion",
    ]
    df = df[cols].sort_values(["fecha", "codigo"]).reset_index(drop=True)

    # reporte
    log(f"\nFilas finales: {len(df):,}  ({len(df)/n0:.1%} de las originales)")
    log(f"Rango de fechas: {df['fecha'].min().date()} → {df['fecha'].max().date()}")
    log(f"Códigos duplicados restantes: {df['codigo'].duplicated().sum()}")
    log("\nSeveridad:\n" + df["severidad"].value_counts().to_string())
    log("\nCobertura de dirección VÁLIDA por severidad (clave para geocodificar):")
    cov = df.groupby("severidad")["direccion_valida"].agg(["sum", "count", "mean"])
    cov.columns = ["con_direccion", "total", "pct"]
    cov["pct"] = (cov["pct"] * 100).round(1)
    log(cov.to_string())
    log(f"Cobertura global de dirección válida: {df['direccion_valida'].mean():.1%}")
    log("\nFilas por año y severidad:")
    log(df.groupby(["anio", "severidad"]).size().unstack(fill_value=0).to_string())
    return df


# ==================================================================== LESIONADOS
def parse_hora(s):
    """Devuelve (hora, minuto). Soporta H:MM, H:MM:SS, 12h con a.m./p.m. y fracción de día."""
    if pd.isna(s):
        return (np.nan, np.nan)
    t = str(s).strip().lower()
    t = re.sub(r"^\d\s{2,}", "", t)           # prefijo basura: '4  1:44:12 p.m.'
    t = re.sub(r"(?<=\d)-\d{2}$", "", t)      # zona horaria: '23:37:08-05'
    if re.fullmatch(r"\d*[.,]\d+", t):        # fracción de día (formato Excel, punto o coma)
        seg = round(float(t.replace(",", ".")) * 86400)
        return ((seg // 3600) % 24, (seg % 3600) // 60)
    m = re.fullmatch(r"(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(?:([ap])\W*m\W*)?", t)
    if not m:
        return (np.nan, np.nan)
    h, mi, ap = int(m.group(1)), int(m.group(2)), m.group(4)
    if ap:  # reloj de 12 horas
        h = h % 12 + (12 if ap == "p" else 0)
    return (h, mi) if h <= 23 and mi <= 59 else (np.nan, np.nan)


def franja(h):
    if pd.isna(h):
        return pd.NA
    return ("Madrugada (0-5)" if h < 6 else "Mañana (6-11)" if h < 12
            else "Tarde (12-17)" if h < 18 else "Noche (18-23)")


def limpiar_lesionados():
    log("\n" + "=" * 70)
    log("DATASET 2: lesionados-en-santiago-de-cali-del-2016-2025.csv")
    log("=" * 70)
    df = leer(F_LES)
    n0 = len(df)
    log(f"Filas originales: {n0:,}")

    df = df.rename(columns={
        "Fecha": "fecha_txt", "Hora": "hora_raw", "Dirección_Reporte": "direccion_original",
        "Tipo_Confirmado.1": "detalle_raw", "Automovil": "actor_automovil",
        "Moto": "actor_moto", "Ciclista": "actor_ciclista", "Peaton": "actor_peaton",
    })
    # Tipo_Confirmado es constante ('Con lesionado'); la placa del agente es un
    # identificador personal, casi siempre vacío y sin valor analítico.
    df = df.drop(columns=["Tipo_Confirmado", "Placa Agente de Transito"])

    antes = len(df)
    df = df.drop_duplicates().reset_index(drop=True)
    log(f"- Duplicados exactos eliminados: {antes - len(df)}")

    # --- FECHA: el archivo cambia de d/m/Y a m/d/Y el 7-sep-2025.
    # El último registro con formato d/m/Y es '6/09/2025'; todo lo posterior es m/d/Y.
    ult_dmy = df.index[df["fecha_txt"] == "6/09/2025"].max()
    es_mdy = df.index > ult_dmy
    f = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")
    f[~es_mdy] = pd.to_datetime(df.loc[~es_mdy, "fecha_txt"], format="%d/%m/%Y", errors="coerce")
    f[es_mdy] = pd.to_datetime(df.loc[es_mdy, "fecha_txt"], format="%m/%d/%Y", errors="coerce")
    df["fecha"] = f
    log(f"- Filas con fecha en formato m/d/Y (desde 7-sep-2025): {es_mdy.sum()}")
    log(f"- Fechas no parseables: {df['fecha'].isna().sum()}")
    cola = df.loc[es_mdy, "fecha"]
    log(f"- Cola m/d/Y cronológica tras corrección: {cola.is_monotonic_increasing}")
    df = df.dropna(subset=["fecha"])

    # --- HORA
    hm = df["hora_raw"].map(parse_hora)
    df["hora"] = hm.map(lambda x: x[0]).astype("Int64")
    df["minuto"] = hm.map(lambda x: x[1]).astype("Int64")
    es12 = df["hora_raw"].fillna("").str.contains(r"[ap]\W*m", case=False)
    esfr = df["hora_raw"].fillna("").str.fullmatch(r"\d*[.,]\d+")
    log(f"- Horas en formato 12h (a.m./p.m.) convertidas: {es12.sum():,}")
    log(f"- Horas en fracción de día (Excel) convertidas: {esfr.sum()}")
    log(f"- Horas no parseables: {df['hora'].isna().sum()}")
    df["franja_horaria"] = df["hora"].map(franja)

    # --- actores viales: '.'→0, 1→1
    actores = ["actor_automovil", "actor_moto", "actor_ciclista", "actor_peaton"]
    for a in actores:
        df[a] = df[a].notna().astype(int)
    df["n_actores"] = df[actores].sum(axis=1)
    df["actor_registrado"] = df["n_actores"] > 0

    # --- detalle y consistencia
    df["detalle_atencion"] = map_cat(df["detalle_raw"], DETALLE, otro="Otro")
    df["detalle_contradictorio"] = df["detalle_atencion"].isin(
        ["Negativo", "Falsa alarma", "No quiere intervención"])

    # --- direcciones
    df["direccion_norm"] = df["direccion_original"].map(norm_direccion)
    df["direccion_valida"] = dir_valida(df["direccion_norm"])
    df["direccion_con_corrupcion"] = df["direccion_original"].fillna("").str.contains(r"\?")

    df = features_fecha(df)
    cols = [
        "fecha", "hora", "minuto", "franja_horaria", "anio", "mes", "dia_semana_num",
        "dia_semana", "es_fin_semana", "es_festivo", "detalle_atencion",
        "detalle_contradictorio", *actores, "n_actores", "actor_registrado",
        "direccion_original", "direccion_norm", "direccion_valida",
        "direccion_con_corrupcion",
    ]
    df = df[cols].sort_values(["fecha", "hora", "minuto"]).reset_index(drop=True)

    log(f"\nFilas finales: {len(df):,}  ({len(df)/n0:.1%} de las originales)")
    log(f"Rango de fechas: {df['fecha'].min().date()} → {df['fecha'].max().date()}")
    log(f"Cobertura de hora válida: {df['hora'].notna().mean():.2%}")
    log(f"Cobertura de dirección válida: {df['direccion_valida'].mean():.1%}")
    log(f"Cobertura de actor vial: {df['actor_registrado'].mean():.1%}")
    log(f"Registros con detalle contradictorio (marcados, no eliminados): "
        f"{df['detalle_contradictorio'].sum()}")
    log("\nLesionados por hora (corregido):")
    log(df["hora"].value_counts().sort_index().to_string())
    log("\nLesionados por franja:\n" + df["franja_horaria"].value_counts().to_string())
    return df


if __name__ == "__main__":
    acc = limpiar_accidentes()
    les = limpiar_lesionados()
    acc.to_csv(OUT_DIR / "siniestralidad_clean.csv", index=False, encoding="utf-8-sig")
    les.to_csv(OUT_DIR / "lesionados_clean.csv", index=False, encoding="utf-8-sig")
    (OUT_DIR / "reporte_limpieza.txt").write_text("\n".join(REPORTE), encoding="utf-8")
    log(f"\nListo → {OUT_DIR}")
