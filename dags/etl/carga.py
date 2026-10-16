"""Carga a MySQL: creación de BD/tablas (idempotente), carga completa y validación."""
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

SCHEMA_SQL = Path(__file__).with_name("schema.sql")
ORDEN = ["dim_fecha", "fact_siniestros", "fact_lesionados",
         "agg_siniestros_mensual", "agg_lesionados_hora"]


def _engine(con_bd=True):
    url = URL.create(
        "mysql+pymysql",
        username=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        host=os.environ.get("MYSQL_HOST", "mysql"),
        port=int(os.environ.get("MYSQL_PORT", 3306)),
        database=os.environ["MYSQL_DATABASE"] if con_bd else None,
        query={"charset": "utf8mb4"},
    )
    return create_engine(url, pool_pre_ping=True)


def crear_bd():
    """Crea la base de datos (si no existe), las tablas y la vista."""
    db = os.environ["MYSQL_DATABASE"]
    eng = _engine(con_bd=False)
    with eng.begin() as c:
        c.execute(text(
            f"CREATE DATABASE IF NOT EXISTS `{db}` "
            "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"))
    eng.dispose()

    sentencias = [s.strip() for s in SCHEMA_SQL.read_text(encoding="utf-8").split(";") if s.strip()]
    eng = _engine()
    with eng.begin() as c:
        for s in sentencias:
            c.execute(text(s))
    eng.dispose()
    print(f"Base '{db}' lista: {len(sentencias)} sentencias DDL ejecutadas")


def _a_sql(df):
    """NA/NaN -> None para que PyMySQL los inserte como NULL."""
    df = df.astype(object)
    return df.where(df.notna(), None)


def cargar(rutas):
    """Carga completa (full refresh): TRUNCATE + INSERT de cada tabla."""
    eng = _engine()
    with eng.begin() as c:
        # TRUNCATE falla sobre tablas referenciadas por FK: se desactiva la
        # revisión solo en esta conexión mientras se vacían las tablas.
        c.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
        for t in ORDEN:
            c.execute(text(f"TRUNCATE TABLE `{t}`"))
        c.execute(text("SET FOREIGN_KEY_CHECKS = 1"))
    cargadas = {}
    for t in ORDEN:
        df = pd.read_parquet(rutas[t])
        _a_sql(df).to_sql(t, eng, if_exists="append", index=False, chunksize=10_000)
        cargadas[t] = len(df)
        print(f"{t}: {len(df):,} filas cargadas")
    eng.dispose()
    return cargadas


def validar(esperadas):
    """Compara el conteo en MySQL contra lo que se cargó."""
    eng = _engine()
    errores = []
    with eng.connect() as c:
        for t, n in esperadas.items():
            real = c.execute(text(f"SELECT COUNT(*) FROM `{t}`")).scalar()
            print(f"{t}: esperadas={n:,} en_mysql={real:,}")
            if real != n:
                errores.append(f"{t}: esperadas {n}, en MySQL {real}")
    eng.dispose()
    if errores:
        raise ValueError("Conteos no coinciden: " + "; ".join(errores))
