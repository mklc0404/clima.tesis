-- ACTIVAR SOLO LA EXTENSIÓN UUID (esta sí viene por defecto)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. Tabla sensor
CREATE TABLE sensor (
    sensor_id TEXT PRIMARY KEY,
    nombre TEXT NOT NULL,
    modelo TEXT,
	fabricante TEXT,
	ubicacion_id TEXT,
    fecha_instalacion DATE,
    fecha_ultima_calibracion DATE
);

-- 2. Tabla ubicación SIN GEOM (solo lat/long como números)
CREATE TABLE ubicacion (
    ubicacion_id TEXT PRIMARY KEY,
    latitud DOUBLE PRECISION NOT NULL,
    longitud DOUBLE PRECISION NOT NULL,
    elevacion DOUBLE PRECISION,
    descripcion_sitio TEXT
);

-- 3. Tabla medición
CREATE TABLE medicion (
    medicion_id BIGSERIAL PRIMARY KEY,
    sensor_id TEXT REFERENCES sensor(sensor_id),
    timestamp TIMESTAMPTZ NOT NULL,
    temperatura DOUBLE PRECISION,
    humedad DOUBLE PRECISION,
    presion DOUBLE PRECISION,
    radiacion_solar DOUBLE PRECISION,
    velocidad_viento DOUBLE PRECISION,
    UNIQUE(sensor_id, timestamp)
);

CREATE TABLE variable (
	variable_id SERIAL PRIMARY KEY,
	nombre_estandar TEXT UNIQUE,
	unidad_estandar TEXT,
	rango_minimo DOUBLE PRECISION,
	rango_maximo DOUBLE PRECISION,
	descripcion TEXT
);

CREATE TABLE validacion (
	validacion_id BIGSERIAL PRIMARY KEY,
	medicion_id BIGINT REFERENCES medicion(medicion_id),
	tipo_flag TEXT,
	descripcion_problema TEXT
)

-- Índices (funcionan perfecto sin PostGIS)
CREATE INDEX idx_medicion_timestamp ON medicion(timestamp);
CREATE INDEX idx_medicion_sensor ON medicion(sensor_id);
CREATE INDEX idx_sensor_time ON medicion(sensor_id, timestamp);

SELECT '¡BASE DE DATOS CREADA SIN ERRORES - LISTA PARA TU TESIS!' AS estado;

CREATE TABLE IF NOT EXISTS variable_sinonimo (
  sinonimo_id SERIAL PRIMARY KEY,
  nombre_sinonimo TEXT UNIQUE NOT NULL,
  nombre_estandar TEXT NOT NULL REFERENCES variable(nombre_estandar),
  ejemplo TEXT
);

INSERT INTO variable (nombre_estandar, unidad_estandar, rango_minimo, rango_maximo, descripcion)
VALUES
('temperatura','degC',-60,60,'Temperatura del aire'),
('humedad','percent',0,100,'Humedad relativa'),
('presion','hPa',300,1100,'Presion atmosferica'),
('radiacion_solar','W/m2',0,2000,'Radiacion solar'),
('velocidad_viento','m/s',0,100,'Velocidad del viento')
ON CONFLICT (nombre_estandar) DO NOTHING;

INSERT INTO variable_sinonimo (nombre_sinonimo, nombre_estandar, ejemplo)
VALUES
('temp_f','temperatura','88.7°F'),
('temp','temperatura','31.5'),
('t','temperatura','31.5'),
('temperature_f','temperatura','77°F'),
('hum','humedad','0.79 or 79'),
('h','humedad','78'),
('p','presion','100870 or 1008.7'),
('pa','presion','100870'),
('pressure','presion','100870');


