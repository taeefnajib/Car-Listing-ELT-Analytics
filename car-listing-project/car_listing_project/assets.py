# import sys
# import os

# from .sql_database_pipeline import load_csv_from_filesystem
# from dagster_embedded_elt.dlt import dlt_assets, DagsterDltResource
# from dagster import AssetExecutionContext

# from dagster import AssetExecutionContext
# from dagster_dbt import DbtCliResource, dbt_assets

# from .project import dbt_project_project

# # Create the pipeline and source once at module level
# pipeline, file_source = load_csv_from_filesystem()

# @dlt_assets(
#     dlt_source=file_source,  # Use the pre-created source
#     dlt_pipeline=pipeline,   # Use the pre-created pipeline
#     group_name="raw",
#     name="raw_csv_to_postgres",
# )
# def dlt_filesystem_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
#     yield from dlt.run(context=context)


# @dbt_assets(manifest=dbt_project_project.manifest_path)
# def dbt_project_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
#     yield from dbt.cli(["build"], context=context).stream()

import sys
import os

from .sql_database_pipeline import csv_filesystem_source, csv_pipeline
from dagster_embedded_elt.dlt import dlt_assets, DagsterDltResource
from dagster import AssetExecutionContext

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from .project import dbt_project_project

@dlt_assets(
    dlt_source=csv_filesystem_source(),  # Call the source function
    dlt_pipeline=csv_pipeline,
    group_name="raw",
    name="raw_csv_to_postgres",
)
def dlt_filesystem_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)

@dbt_assets(manifest=dbt_project_project.manifest_path)
def dbt_project_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dlt.cli(["build"], context=context).stream()