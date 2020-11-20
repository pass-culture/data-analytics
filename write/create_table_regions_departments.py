import pandas as pd
import sqlalchemy
import os

write_path = os.path.dirname(os.path.realpath(__file__))
REGIONS_DEPARTMENTS_FILE = write_path + "/data/departements-regions.csv"


def create_table_regions_departments(
    regions_departments_dataframe: pd.DataFrame, ENGINE
) -> None:
    with ENGINE.connect() as connection:
        regions_departments_dataframe.to_sql(
            name="regions_departments",
            con=connection,
            if_exists="replace",
            dtype={
                "num_dep": sqlalchemy.types.VARCHAR(length=250),
                "dep_name": sqlalchemy.types.VARCHAR(length=250),
                "region_name": sqlalchemy.types.VARCHAR(length=250),
            },
        )


def create_table_regions_departments_data(ENGINE) -> None:
    with open(REGIONS_DEPARTMENTS_FILE, "r") as file:
        regions_departments_dataframe = pd.read_csv(file)

    create_table_regions_departments(regions_departments_dataframe, ENGINE)
