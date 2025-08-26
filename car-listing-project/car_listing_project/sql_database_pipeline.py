# import dlt
# from dlt.sources.filesystem import filesystem, read_csv

# def load_csv_from_filesystem():
#     """Use the filesystem source to load a CSV file."""
#     # Configure the source to load the CSV file
#     # Make sure the CSV file exists in the path specified in secrets.toml
#     file_source = filesystem(
#         file_glob="bikroy_cars_raw.csv"
#     )
#     # Create a pipeline that will load to a postgres destination
#     pipeline = dlt.pipeline(
#         pipeline_name="car_listing_raw",
#         destination='postgres',
#         dataset_name="raw_data"
#     )
#     return pipeline, file_source


# import dlt
# from dlt.sources.filesystem import filesystem, read_csv

# @dlt.source
# def csv_filesystem_source():
#     """DLT source for loading CSV files from filesystem"""
#     # Define your filesystem source
#     file_source = filesystem(
#         bucket_url="H:\\DA\\top-5\\car-listing-elt-analytics\\raw",  # Update this path
#         file_glob="*.csv"
#     )
    
#     return file_source

# # Create the pipeline
# csv_pipeline = dlt.pipeline(
#     pipeline_name="csv_filesystem_pipeline",
#     destination="postgres",
#     dataset_name="raw",    # explicitly set
#     dev_mode=True     # optional: clears state so you re-load
# )


# if __name__ == "__main__":
#     info = csv_pipeline.run(csv_filesystem_source() | read_csv().with_name("csv_files"))
#     print(info)

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
