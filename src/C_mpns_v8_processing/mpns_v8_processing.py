import os
from pathlib import Path
from typing import List

from pyspark.sql import SparkSession, functions as f
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, BooleanType


from input_schemas import (
    MPNS_V8_PLANTS,
    MPNS_V8_SYNONYMS,
    MPNS_V8_NON_SCIENTIFIC_NAMES,
)

from output_schemas import OUTPUT_SCHEMA


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

    # Load the three files into DataFrames
    plants_df: DataFrame = load_for_schema(
        spark,
        input_filepath=os.path.join(input_filepath, "medicinal_mpns_plants.csv"),
        schema=MPNS_V8_PLANTS,
        delimiter="|",
    )
    synonyms_df: DataFrame = load_for_schema(
        spark,
        input_filepath=os.path.join(input_filepath, "medicinal_mpns_synonyms.csv"),
        schema=MPNS_V8_SYNONYMS,
        delimiter="|",
    )
    non_scientific_names_df: DataFrame = load_for_schema(
        spark,
        input_filepath=os.path.join(
            input_filepath, "medicinal_mpns_non_scientific_names.csv"
        ),
        schema=MPNS_V8_NON_SCIENTIFIC_NAMES,
        delimiter=";",
    )

    # Filter plants DataFrame, join to non-scientific names,
    # construct the name mappings from this.
    filtered_plants_df: DataFrame = filter_exclusions(
        df=plants_df,
        exclude_quality_rating=exclude_quality_rating,
        exclude_taxon_status=exclude_taxon_status,
    )

    plants_to_non_scientific_names_df: DataFrame = filtered_plants_df.join(
        non_scientific_names_df,
        filtered_plants_df.name_id == non_scientific_names_df.plant_id,
        "left",
    ).drop(non_scientific_names_df.name_id)

    plants_to_non_scientific_names_cols: List = [
        "name_id",
        "full_scientific_name",
        "name",
        "name_type",
    ]
    plants_to_non_scientific_names_df: DataFrame = (
        plants_to_non_scientific_names_df.select(*plants_to_non_scientific_names_cols)
    )

    plants_to_non_scientific_names_df: DataFrame = construct_name_mappings_df(
        df=plants_to_non_scientific_names_df, is_synonym_filetype=False
    )

    # Filter synonyms DataFrame, join to non-scientific names,
    # construct the name mappings from this.
    filtered_synonyms_df: DataFrame = filter_exclusions(
        df=synonyms_df,
        exclude_quality_rating=exclude_quality_rating,
        exclude_taxon_status=exclude_taxon_status,
    )

    synonyms_to_non_scientific_names_df: DataFrame = filtered_synonyms_df.join(
        non_scientific_names_df,
        filtered_synonyms_df.acc_name_id == non_scientific_names_df.plant_id,
        "left",
    ).drop(non_scientific_names_df.name_id)

    synonyms_to_non_scientific_names_cols: List = [
        "name_id",
        "full_scientific_name",
        "name",
        "name_type",
    ]
    synonyms_to_non_scientific_names_df: DataFrame = (
        synonyms_to_non_scientific_names_df.select(
            *synonyms_to_non_scientific_names_cols
        )
    )

    synonyms_to_non_scientific_names_df: DataFrame = construct_name_mappings_df(
        df=synonyms_to_non_scientific_names_df, is_synonym_filetype=True
    )

    # Join both name mappings DataFrames for everything
    all_name_mappings_df: DataFrame = plants_to_non_scientific_names_df.union(
        synonyms_to_non_scientific_names_df
    )

    # Add a unique mapping_id - won't be deterministic with each run!
    all_name_mappings_df: DataFrame = all_name_mappings_df.withColumn(
        "mapping_id",
        f.row_number().over(Window.orderBy("full_scientific_name")),
    )

    # Write name mappings to JSON files
    write_name_mappings_to_json(
        df=all_name_mappings_df, output_filepath=output_filepath
    )


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


def construct_name_mappings_df(df: DataFrame, is_synonym_filetype: bool) -> DataFrame:
    return (
        df.withColumnRenamed("name_id", "full_scientific_name_id")
        .withColumnRenamed("name", "non_scientific_name")
        .withColumnRenamed("name_type", "non_scientific_name_type")
        .withColumn("is_synonym", f.lit(is_synonym_filetype).cast(BooleanType()))
    )


def write_name_mappings_to_json(df: DataFrame, output_filepath: str) -> None:
    output_filepath_path: Path = Path(output_filepath).parents[0]
    output_filepath_path.mkdir(parents=True, exist_ok=True)
    df.coalesce(1).write.format("json").mode("overwrite").option(
        "schema", OUTPUT_SCHEMA
    ).save(output_filepath)


def write_process_metadata(output_filepath: Path) -> None:
    pass


mpns_raw_filepath = "data/mpns/sample_mpns_v8/"
mpns_processed_filepath = "data/processed/mpns/sample_mpns_v8/mpns_name_mappings/"
process_mpns_v8_raw(
    input_filepath=mpns_raw_filepath,
    output_filepath=mpns_processed_filepath,
    exclude_quality_rating=["L"],
    exclude_taxon_status=["Misapplied"],
)
