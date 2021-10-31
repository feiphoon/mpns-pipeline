import os
import json
from functools import reduce
from pathlib import Path
from typing import List

from pyspark.sql import SparkSession, functions as f
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import StructType


from input_schemas import (
    MPNS_V8_PLANTS,
    MPNS_V8_SYNONYMS,
    MPNS_V8_NON_SCIENTIFIC_NAMES,
)

from output_schemas import OUTPUT_SCHEMA_V2


#  Monkeypatch in case I don't use Spark 3.0
def transform(self, f):
    return f(self)


DataFrame.transform = transform


def process_mpns_v8_raw(
    input_filepath: Path,
    output_filepath: Path,
    exclude_quality_rating: List[str],
    exclude_taxon_status: List[str],
    sample_run: bool,
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

    # Separate sci_cited_medicinal_names from common & pharmaceutical names
    sci_cited_medicinal_names_df: DataFrame = non_scientific_names_df.filter(
        f.col("name_type") == "sci_cited_medicinal"
    )

    common_and_pharmaceutical_names_df: DataFrame = non_scientific_names_df.filter(
        f.col("name_type") != "sci_cited_medicinal"
    )

    # Create three name mapping DataFrames
    plants_to_common_and_pharmaceutical_names_df = (
        create_plants_to_common_and_pharmaceutical_names_df(
            plants_df=plants_df,
            common_and_pharmaceutical_names_df=common_and_pharmaceutical_names_df,
            exclude_quality_rating=exclude_quality_rating,
            exclude_taxon_status=exclude_taxon_status,
        )
    )

    # print(plants_to_common_and_pharmaceutical_names_df.show(truncate=False))

    synonyms_to_common_and_pharmaceutical_names_df = (
        create_synonyms_to_common_and_pharmaceutical_names_df(
            synonyms_df=synonyms_df,
            common_and_pharmaceutical_names_df=common_and_pharmaceutical_names_df,
            exclude_quality_rating=exclude_quality_rating,
            exclude_taxon_status=exclude_taxon_status,
        )
    )

    # print(synonyms_to_common_and_pharmaceutical_names_df.show(truncate=False))

    sci_cited_medicinal_to_common_and_pharmaceutical_names_df = (
        create_sci_cited_medicinal_to_common_and_pharmaceutical_names_df(
            plants_df=plants_df,
            sci_cited_medicinal_names_df=sci_cited_medicinal_names_df,
            common_and_pharmaceutical_names_df=common_and_pharmaceutical_names_df,
            exclude_quality_rating=exclude_quality_rating,
            exclude_taxon_status=exclude_taxon_status,
        )
    )

    # print(
    #     sci_cited_medicinal_to_common_and_pharmaceutical_names_df.show(truncate=False)
    # )

    # Join all three name mappings DataFrames for everything
    _dfs_to_union: list = [
        plants_to_common_and_pharmaceutical_names_df,
        synonyms_to_common_and_pharmaceutical_names_df,
        sci_cited_medicinal_to_common_and_pharmaceutical_names_df,
    ]

    all_name_mappings_df: DataFrame = reduce(DataFrame.union, _dfs_to_union)

    # Add a unique mapping_id - won't be deterministic with each run!
    all_name_mappings_df: DataFrame = all_name_mappings_df.withColumn(
        "mapping_id",
        f.row_number().over(Window.orderBy("scientific_name")),
    )

    # Write name mappings to JSON files
    write_name_mappings_to_file(
        df=all_name_mappings_df, output_filepath=output_filepath, sample_run=sample_run
    )

    print(all_name_mappings_df.show(truncate=False))

    write_process_metadata(df=all_name_mappings_df, output_filepath=output_filepath)


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


def create_plants_to_common_and_pharmaceutical_names_df(
    plants_df: DataFrame,
    common_and_pharmaceutical_names_df: DataFrame,
    exclude_quality_rating: List[str],
    exclude_taxon_status: List[str],
) -> DataFrame:
    # Filter plants DataFrame, join to common & pharmaceutical names,
    # construct the name mappings from this.
    filtered_plants_df: DataFrame = (
        filter_exclusions(
            df=plants_df,
            exclude_quality_rating=exclude_quality_rating,
            exclude_taxon_status=exclude_taxon_status,
        )
        .withColumnRenamed("name_id", "scientific_name_id")
        .withColumnRenamed("full_scientific_name", "scientific_name")
    )

    plants_to_common_and_pharmaceutical_names_df: DataFrame = filtered_plants_df.join(
        common_and_pharmaceutical_names_df,
        filtered_plants_df.scientific_name_id
        == common_and_pharmaceutical_names_df.plant_id,
        "left",
    )

    plants_to_common_and_pharmaceutical_names_df_cols: List = [
        "scientific_name_id",
        "scientific_name",
        "name_id",
        "name",
        "name_type",
    ]
    plants_to_common_and_pharmaceutical_names_df: DataFrame = (
        plants_to_common_and_pharmaceutical_names_df.select(
            *plants_to_common_and_pharmaceutical_names_df_cols
        )
    )

    return construct_name_mappings_df(
        df=plants_to_common_and_pharmaceutical_names_df,
        scientific_name_type="plant",
    )


def create_synonyms_to_common_and_pharmaceutical_names_df(
    synonyms_df: DataFrame,
    common_and_pharmaceutical_names_df: DataFrame,
    exclude_quality_rating: List[str],
    exclude_taxon_status: List[str],
) -> DataFrame:
    # Filter synonyms DataFrame, join to common & pharmaceutical names,
    # construct the name mappings from this.
    filtered_synonyms_df: DataFrame = (
        filter_exclusions(
            df=synonyms_df,
            exclude_quality_rating=exclude_quality_rating,
            exclude_taxon_status=exclude_taxon_status,
        )
        .withColumnRenamed("name_id", "scientific_name_id")
        .withColumnRenamed("full_scientific_name", "scientific_name")
    )

    synonyms_to_common_and_pharmaceutical_names_df: DataFrame = (
        filtered_synonyms_df.join(
            common_and_pharmaceutical_names_df,
            filtered_synonyms_df.acc_name_id
            == common_and_pharmaceutical_names_df.plant_id,
            "left",
        )
    )

    synonyms_to_common_and_pharmaceutical_names_cols: List = [
        "scientific_name_id",
        "scientific_name",
        "name_id",
        "name",
        "name_type",
    ]
    synonyms_to_common_and_pharmaceutical_names_df: DataFrame = (
        synonyms_to_common_and_pharmaceutical_names_df.select(
            *synonyms_to_common_and_pharmaceutical_names_cols
        )
    )

    return construct_name_mappings_df(
        df=synonyms_to_common_and_pharmaceutical_names_df,
        scientific_name_type="synonym",
    )


def create_sci_cited_medicinal_to_common_and_pharmaceutical_names_df(
    plants_df: DataFrame,
    sci_cited_medicinal_names_df: DataFrame,
    common_and_pharmaceutical_names_df: DataFrame,
    exclude_quality_rating: List[str],
    exclude_taxon_status: List[str],
) -> DataFrame:
    # Get only the plants which meet the minimum standard of exclusions
    # So we can right join it to the sci_cited_medicinal names
    # to ensure we only use correct ones.
    filtered_plants_df: DataFrame = (
        filter_exclusions(
            df=plants_df,
            exclude_quality_rating=exclude_quality_rating,
            exclude_taxon_status=exclude_taxon_status,
        )
        .select("name_id")
        .withColumnRenamed("name_id", "filtered_plants_name_id")
    )

    sci_cited_medicinal_names_df: DataFrame = sci_cited_medicinal_names_df.join(
        filtered_plants_df,
        sci_cited_medicinal_names_df.plant_id
        == filtered_plants_df.filtered_plants_name_id,
        "right",
    ).drop("filtered_plants_name_id")

    # Join sci_cited_medicinal_names to to common & pharmaceutical names.
    # Note that the plant_id here will be a full_scientific_name_id,
    # or the name_id that matches an entry in the plants table.
    # So the plant_id is joined to plant_id here.
    # The rename here are just to make this consistent to use
    # the construct_name_mappings_df() function. The order is important.
    sci_cited_medicinal_names_df: DataFrame = (
        sci_cited_medicinal_names_df.withColumnRenamed("name_id", "scientific_name_id")
        .withColumnRenamed("name", "scientific_name")
        .drop("name_type")
    )
    sci_cited_medicinal_to_common_and_pharmaceutical_names_df: DataFrame = (
        sci_cited_medicinal_names_df.join(
            common_and_pharmaceutical_names_df,
            sci_cited_medicinal_names_df.plant_id
            == common_and_pharmaceutical_names_df.plant_id,
            "left",
        ).drop(
            common_and_pharmaceutical_names_df.plant_id,
        )
    )

    sci_cited_medicinal_to_common_and_pharmaceutical_names_cols: List = [
        "scientific_name_id",
        "scientific_name",
        "name_id",
        "name",
        "name_type",
    ]
    sci_cited_medicinal_to_common_and_pharmaceutical_names_df: DataFrame = (
        sci_cited_medicinal_to_common_and_pharmaceutical_names_df.select(
            *sci_cited_medicinal_to_common_and_pharmaceutical_names_cols
        )
    )

    return construct_name_mappings_df(
        df=sci_cited_medicinal_to_common_and_pharmaceutical_names_df,
        scientific_name_type="sci_cited_medicinal",
    )


def construct_name_mappings_df(df: DataFrame, scientific_name_type: str) -> DataFrame:
    return (
        df.withColumnRenamed("name_id", "non_scientific_name_id")
        .withColumnRenamed("name", "non_scientific_name")
        .withColumnRenamed("name_type", "non_scientific_name_type")
        .withColumn("scientific_name_type", f.lit(scientific_name_type))
    )


def write_name_mappings_to_file(
    df: DataFrame, output_filepath: str, sample_run: bool
) -> None:
    output_filepath_path: Path = Path(output_filepath).parents[0]
    output_filepath_path.mkdir(parents=True, exist_ok=True)
    if sample_run:
        # Coalesce to 1 JSON file for sample demonstration
        df.coalesce(1).write.format("json").mode("overwrite").option(
            "schema", OUTPUT_SCHEMA_V2
        ).save(output_filepath)

    else:
        # Repartition to ballpark of 5 parquet files for real data
        df.repartition(5).write.mode("overwrite").option(
            "schema", OUTPUT_SCHEMA_V2
        ).parquet(output_filepath)


def write_process_metadata(df: DataFrame, output_filepath: Path) -> None:
    total_count: int = df.count()
    plant_name_count: int = df.filter(f.col("scientific_name_type") == "plant").count()
    synonym_name_count: int = df.filter(
        f.col("scientific_name_type") == "synonym"
    ).count()
    sci_cited_medicinal_name_count: int = df.filter(
        f.col("scientific_name_type") == "sci_cited_medicinal"
    ).count()
    common_name_count: int = df.filter(
        f.col("non_scientific_name_type") == "common"
    ).count()
    pharmaceutical_name_count: int = df.filter(
        f.col("non_scientific_name_type") == "pharmaceutical"
    ).count()

    _metadata = {
        "total_count": total_count,
        "plant_name_count": plant_name_count,
        "synonym_name_count": synonym_name_count,
        "common_name_count": common_name_count,
        "pharmaceutical_name_count": pharmaceutical_name_count,
        "sci_cited_medicinal_name_count": sci_cited_medicinal_name_count,
    }

    with Path(f"{output_filepath}/ process_metadata.json").open(
        "w", encoding="utf-8"
    ) as file:
        json.dump(_metadata, file)


# TODO: Use argparse to pass sample_run as a flag to container.

# These are here for demonstration purposes
sample_mpns_raw_filepath = "data/mpns/sample_mpns_v8/"
sample_mpns_processed_filepath = (
    "data/processed/mpns/sample_mpns_v8/mpns_name_mappings/v2/"
)
process_mpns_v8_raw(
    input_filepath=sample_mpns_raw_filepath,
    output_filepath=sample_mpns_processed_filepath,
    exclude_quality_rating=["L"],
    exclude_taxon_status=["Misapplied"],
    sample_run=True,
)

# mpns_raw_filepath = "data/mpns/mpns_v8/"
# mpns_processed_filepath = "data/processed/mpns/mpns_v8/mpns_name_mappings/v2/"
# process_mpns_v8_raw(
#     input_filepath=mpns_raw_filepath,
#     output_filepath=mpns_processed_filepath,
#     exclude_quality_rating=["L"],
#     exclude_taxon_status=["Misapplied"],
#     sample_run=False,
# )
