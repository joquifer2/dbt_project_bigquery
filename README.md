# Proyecto dbt para Bitrix CRM y Ads en BigQuery

Este repositorio contiene un proyecto dbt para modelado y análisis de datos de campañas, leads y ventas en Google BigQuery, integrando fuentes de Bitrix CRM y plataformas de Ads (Google, Meta, etc.).

---

## 🚀 Instalación y entorno

### 1. Requisitos previos
- Python 3.10+
- [Poetry](https://python-poetry.org/docs/#installation)
- Acceso a Google BigQuery y una cuenta de servicio con permisos adecuados

### 2. Clona el repositorio
```bash
# Clona tu fork o el repo original
git clone https://github.com/tu_usuario/tu_repo.git
cd tu_repo
```

### 3. Instala las dependencias de Python
```bash
poetry install
```

### 4. Configura las credenciales de BigQuery
Coloca tu archivo de credenciales (por ejemplo, `credentials.json`) en la carpeta `credentials/` (esta carpeta está en `.gitignore` y no se sube al repo).

Configura tu perfil de conexión en `~/.dbt/profiles.yml`:
```yaml
bkm:
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: <tu-proyecto-gcp>
      dataset: <tu-dataset>
      keyfile: <ruta-a-tus-credenciales>
      location: EU
      threads: 4
  target: dev
```

---

## 📁 Estructura del proyecto

- `models/` — Modelos dbt organizados por dominio (`staging/`, `intermediate/`, `marts/`)
- `seeds/` — Tablas semilla (dimensiones, catálogos)
- `macros/` — Macros personalizadas de dbt
- `logs/`, `target/`, `dbt_packages/` — Carpetas generadas automáticamente (ignoradas en git)
- `pyproject.toml`, `poetry.lock` — Gestión de dependencias Python

---

## 🛠️ Comandos útiles

- Ejecutar todos los modelos:
  ```bash
  poetry run dbt run
  ```
- Ejecutar tests:
  ```bash
  poetry run dbt test
  ```
- Generar documentación:
  ```bash
  poetry run dbt docs generate
  ```
- Servir la documentación localmente:
  ```bash
  poetry run dbt docs serve
  ```

---

## 📝 Buenas prácticas
- No subas credenciales ni archivos sensibles al repositorio.
- Usa `materialized='view'` para staging y `materialized='table'` para modelos finales.
- Documenta y testea todos los modelos y seeds con YAMLs.
- Mantén el entorno reproducible usando Poetry y el lockfile.

---

## 📣 Contacto
Autor: Jordi Quiroga — [jordi@jordiquiroga.com](mailto:jordi@jordiquiroga.com)

---

Este README resume la estructura, instalación y buenas prácticas del proyecto dbt para Bitrix CRM y Ads en BigQuery.
