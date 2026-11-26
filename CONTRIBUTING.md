# Cómo contribuir

## Reportar un problema

Abre un *issue* con un título claro y describe qué esperabas y qué ocurrió. Incluye los pasos para reproducirlo, el notebook o consulta afectada y, si aplica, el mensaje de error. No publiques credenciales ni datos sensibles.

## Validar cambios

Para cambios en notebooks, usa un workspace de Databricks con los permisos y recursos necesarios. Ejecuta todas las celdas en orden, siguiendo el flujo del proyecto:

1. `notebooks/01_ingest_online_retail_bronze.ipynb`
2. `notebooks/02_build_silver_retail_sales_clean.ipynb`
3. `notebooks/03_build_gold_analytics_model.ipynb`

Después, ejecuta `sql/03_validation_queries.sql.dbquery.ipynb` y confirma que las validaciones no reporten diferencias ni errores. Si no puedes ejecutar los notebooks, indícalo en el pull request.

## Enviar un pull request

Trabaja en una rama y abre un pull request hacia `main`. Explica qué cambiaste y por qué, e indica las validaciones realizadas y sus resultados. Para cambios en dashboards, incluye capturas de pantalla cuando sea útil. Mantén el cambio enfocado y revisa que no incluya credenciales ni datos sensibles.
