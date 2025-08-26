from dagster import Definitions, ScheduleDefinition, AssetSelection, DefaultScheduleStatus, define_asset_job
from dagster_embedded_elt.dlt import DagsterDltResource
from dagster_dbt import DbtCliResource
from car_listing_project.assets import dlt_filesystem_assets, dbt_project_dbt_assets
from car_listing_project.project import dbt_project_project

dlt_resource = DagsterDltResource()

# Define jobs for each asset
dlt_job = define_asset_job(name="dlt_job", selection=AssetSelection.assets(dlt_filesystem_assets))
dbt_job = define_asset_job(name="dbt_job", selection=AssetSelection.assets(dbt_project_dbt_assets))

# Define schedules
saas_schedule = ScheduleDefinition(
    name="saas_schedule",
    job=dlt_job,
    cron_schedule="*/5 * * * *",
    default_status=DefaultScheduleStatus.RUNNING,
)

dbt_schedule = ScheduleDefinition(
    name="dbt_schedule",
    job=dbt_job,
    cron_schedule="*/10 * * * *",
    default_status=DefaultScheduleStatus.RUNNING,
)

defs = Definitions(
    assets=[
        dlt_filesystem_assets,
        dbt_project_dbt_assets
    ],
    resources={
        "dlt": dlt_resource,
        "dbt": DbtCliResource(project_dir=dbt_project_project),
    },
    # schedules=[saas_schedule, dbt_schedule],
    # jobs=[dlt_job, dbt_job],
)
