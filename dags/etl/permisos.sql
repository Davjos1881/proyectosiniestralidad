CREATE DATABASE IF NOT EXISTS siniestralidad_cali
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

GRANT SELECT, INSERT, UPDATE, DELETE,
      CREATE, ALTER, DROP, INDEX, REFERENCES,
      CREATE VIEW, SHOW VIEW
ON siniestralidad_cali.*
TO 'davidsp'@'localhost';

FLUSH PRIVILEGES;

SHOW GRANTS FOR 'davidsp'@'localhost';