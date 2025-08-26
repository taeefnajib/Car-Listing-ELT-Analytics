import dlt
from dlt.sources.filesystem import filesystem, read_csv

@dlt.source
def csv_filesystem_source():
    return filesystem(
        bucket_url=r"H:\DA\top-5\car-listing-elt-analytics\raw",
        file_glob="*.csv"
    ) | read_csv().with_name("car_listings")

# Create the pipeline
csv_pipeline = dlt.pipeline(
    pipeline_name="csv_filesystem_pipeline",
    destination="postgres",
    dataset_name="raw"
)

if __name__ == "__main__":
    info = csv_pipeline.run(csv_filesystem_source())
    print(info)
