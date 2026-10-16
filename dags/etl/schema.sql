CREATE TABLE IF NOT EXISTS dim_fecha (
    fecha            DATE PRIMARY KEY,
    anio             SMALLINT NOT NULL,
    mes              TINYINT NOT NULL,
    nombre_mes       VARCHAR(12) NOT NULL,
    trimestre        TINYINT NOT NULL,
    anio_mes         CHAR(7) NOT NULL,
    dia              TINYINT NOT NULL,
    dia_semana_num   TINYINT NOT NULL,
    dia_semana       VARCHAR(12) NOT NULL,
    es_fin_semana    TINYINT(1) NOT NULL,
    es_festivo       TINYINT(1) NOT NULL,
    nombre_festivo   VARCHAR(100) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS fact_siniestros (
    siniestro_id            INT AUTO_INCREMENT PRIMARY KEY,
    codigo                  VARCHAR(50),
    fecha                   DATE NOT NULL,
    anio                    SMALLINT,
    mes                     TINYINT,
    dia_semana_num          TINYINT,
    dia_semana              VARCHAR(12),
    es_fin_semana           TINYINT(1),
    es_festivo              TINYINT(1),
    severidad               VARCHAR(20),
    fallecido_foraneo       TINYINT(1),
    clase_accidente         VARCHAR(40),
    detalle_atencion        VARCHAR(100),
    medio_reporte           VARCHAR(40),
    num_vehiculos           INT NULL,
    vehiculos_implicados    TEXT NULL,
    veh_moto                TINYINT(1) NULL,
    veh_motocarro           TINYINT(1) NULL,
    veh_automovil           TINYINT(1) NULL,
    veh_camioneta_campero   TINYINT(1) NULL,
    veh_bus                 TINYINT(1) NULL,
    veh_carga_pesada        TINYINT(1) NULL,
    veh_bicicleta           TINYINT(1) NULL,
    direccion_original      VARCHAR(500) NULL,
    direccion_norm          VARCHAR(500) NULL,
    direccion_valida        TINYINT(1),
    direccion_con_corrupcion TINYINT(1),
    INDEX idx_sin_fecha (fecha),
    INDEX idx_sin_severidad (severidad),
    INDEX idx_sin_codigo (codigo),
    CONSTRAINT fk_sin_fecha FOREIGN KEY (fecha) REFERENCES dim_fecha (fecha)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS fact_lesionados (
    lesionado_id            INT AUTO_INCREMENT PRIMARY KEY,
    fecha                   DATE NOT NULL,
    hora                    TINYINT NULL,
    minuto                  TINYINT NULL,
    franja_horaria          VARCHAR(20) NULL,
    anio                    SMALLINT,
    mes                     TINYINT,
    dia_semana_num          TINYINT,
    dia_semana              VARCHAR(12),
    es_fin_semana           TINYINT(1),
    es_festivo              TINYINT(1),
    detalle_atencion        VARCHAR(100),
    detalle_contradictorio  TINYINT(1),
    actor_automovil         TINYINT(1),
    actor_moto              TINYINT(1),
    actor_ciclista          TINYINT(1),
    actor_peaton            TINYINT(1),
    n_actores               TINYINT,
    actor_registrado        TINYINT(1),
    direccion_original      VARCHAR(500) NULL,
    direccion_norm          VARCHAR(500) NULL,
    direccion_valida        TINYINT(1),
    direccion_con_corrupcion TINYINT(1),
    INDEX idx_les_fecha (fecha),
    INDEX idx_les_hora (hora),
    CONSTRAINT fk_les_fecha FOREIGN KEY (fecha) REFERENCES dim_fecha (fecha)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS agg_siniestros_mensual (
    anio             SMALLINT,
    mes              TINYINT,
    severidad        VARCHAR(20),
    clase_accidente  VARCHAR(40),
    n_siniestros     INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS agg_lesionados_hora (
    anio             SMALLINT,
    dia_semana_num   TINYINT,
    dia_semana       VARCHAR(12),
    hora             TINYINT,
    n_lesionados     INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE OR REPLACE VIEW vw_resumen_diario AS
SELECT d.fecha, d.anio, d.mes, d.nombre_mes, d.dia_semana, d.es_fin_semana, d.es_festivo,
       COALESCE(s.n, 0) AS siniestros,
       COALESCE(l.n, 0) AS lesionados
FROM dim_fecha d
LEFT JOIN (SELECT fecha, COUNT(*) AS n FROM fact_siniestros GROUP BY fecha) s ON s.fecha = d.fecha
LEFT JOIN (SELECT fecha, COUNT(*) AS n FROM fact_lesionados GROUP BY fecha) l ON l.fecha = d.fecha
