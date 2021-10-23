import os
from pathlib import Path
from typing import List

from pyspark.sql import SparkSession, functions as f

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType


from input_schemas import (
    MPNS_V8_PLANTS,
    MPNS_V8_SYNONYMS,
    MPNS_V8_NON_SCIENTIFIC_NAMES,
)


#  Monkeypatch in case I don't use Spark 3.0
def transform(self, f):
    return f(self)


DataFrame.transform = transform


def process_mpns_v8_raw(
    input_filepath: Path,
    output_filepath: Path,
    exclude_quality_rating: List[str],
    exclude_taxon_status: List[str],
) -> None:
    spark = SparkSession.builder.appName("process_mpns_v8_raw").getOrCreate()

    plants_df = load_for_schema(
        spark,
        input_filepath=os.path.join(input_filepath, "medicinal_mpns_plants.csv"),
        schema=MPNS_V8_PLANTS,
        delimiter="|",
    )
    synonyms_df = load_for_schema(
        spark,
        input_filepath=os.path.join(input_filepath, "medicinal_mpns_synonyms.csv"),
        schema=MPNS_V8_SYNONYMS,
        delimiter="|",
    )

    non_scientific_names_df = load_for_schema(
        spark,
        input_filepath=os.path.join(
            input_filepath, "medicinal_mpns_non_scientific_names.csv"
        ),
        schema=MPNS_V8_NON_SCIENTIFIC_NAMES,
        delimiter=";",
    )

    filtered_plants_df = filter_exclusions(
        df=plants_df,
        exclude_quality_rating=exclude_quality_rating,
        exclude_taxon_status=exclude_taxon_status,
    )

    plants_to_non_scientific_names_df = filtered_plants_df.join(
        non_scientific_names_df,
        filtered_plants_df.name_id == non_scientific_names_df.plant_id,
        "left",
    )

    plants_to_non_scientific_names_cols = [
        "name_id",
        "full_scientific_name",
        "name",
        "name_type",
    ]
    plants_to_non_scientific_names_df = plants_to_non_scientific_names_df.select(
        *plants_to_non_scientific_names_cols
    )

    plants_to_non_scientific_names_df = (
        plants_to_non_scientific_names_df.withColumnRenamed(
            "name_id", "full_scientific_name_id"
        )
        .withColumnRenamed("name", "non_scientific_name")
        .withColumnRenamed("name_type", "non_scientific_name_type")
    )

    filtered_synonyms_df = filter_exclusions(
        df=synonyms_df,
        exclude_quality_rating=exclude_quality_rating,
        exclude_taxon_status=exclude_taxon_status,
    )

    synonyms_to_non_scientific_names_df = filtered_synonyms_df.join(
        non_scientific_names_df,
        filtered_synonyms_df.acc_name_id == non_scientific_names_df.plant_id,
        "left",
    )

    synonyms_to_non_scientific_names_cols = [
        "name_id",
        "full_scientific_name",
        "name",
        "name_type",
    ]
    synonyms_to_non_scientific_names_df = synonyms_to_non_scientific_names_df.select(
        *synonyms_to_non_scientific_names_cols
    )

    synonyms_to_non_scientific_names_df = (
        synonyms_to_non_scientific_names_df.withColumnRenamed(
            "name_id", "full_scientific_name_id"
        )
        .withColumnRenamed("name", "non_scientific_name")
        .withColumnRenamed("name_type", "non_scientific_name_type")
    )

    print("p nsn", plants_to_non_scientific_names_df.show(3, truncate=False))
    print("s nsn", synonyms_to_non_scientific_names_df.show(3, truncate=False))


def load_for_schema(
    spark: SparkSession, input_filepath: str, schema: StructType, delimiter: str
) -> DataFrame:
    return (
        spark.read.option("delimiter", delimiter)
        .format("csv")
        .schema(schema)
        .option("header", True)
        .load(input_filepath)
    )


def filter_exclusions(
    df: DataFrame,
    exclude_quality_rating: List[str] = [],
    exclude_taxon_status: List[str] = [],
) -> DataFrame:
    # If both exclusion lists contain no items, short-circuit
    if (not exclude_quality_rating) and (not exclude_taxon_status):
        return df

    if exclude_quality_rating:
        for _quality_status in exclude_quality_rating:
            df = df.filter(f.col("quality_rating") != _quality_status)

    if exclude_taxon_status:
        for _taxon_status in exclude_taxon_status:
            df = df.filter(f.col("taxon_status") != _taxon_status)

    return df


process_mpns_v8_raw(
    input_filepath=Path("data/mpns/mpns_v8"),
    output_filepath=Path("potato"),
    exclude_quality_rating=["L"],
    exclude_taxon_status=["Misapplied"],
)
