"""
This version of MPNS processing has a minor difference - we group mappings again
but in a different way to make our life easier downstream, and we precalculate
the lengths of each name, anticipating replacement later.
"""
import os
import json
import string
from functools import reduce
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

from output_schemas import OUTPUT_SCHEMA_V5


#  Monkeypatch in case I don't use Spark 3.0
def transform(self, f):
    return f(self)


DataFrame.transform = transform


def process_mpns_v8_raw(
    input_filepath: str,
    output_filepath: str,
    exclude_quality_rating: List[str],
    exclude_taxon_status: List[str],
    sample_run: bool,
) -> None:
    spark = SparkSession.builder.appName("process_mpns_v8_raw").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

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

    # Filter out non_scientific_names which are not using Latin alphabet.
    # This is because we get into some trouble with replacing entities as they
    # can be written in unicode to JSON and disrupt entity replacement.
    non_scientific_names_df: DataFrame = non_scientific_names_df.withColumn(
        "has_only_allowed_characters",
        check_if_contains_only_allowed_characters_udf(f.col("name")),
    )

    non_scientific_names_df: DataFrame = non_scientific_names_df.filter(
        f.col("has_only_allowed_characters")
    )

    # print(non_scientific_names_df.show(20, truncate=False))

    common_names_df: DataFrame = (
        non_scientific_names_df.filter(f.col("name_type") == "common")
        .transform(transform_non_scientific_names)
        .transform(lambda df: construct_non_scientific_name_struct(df, "common_names"))
    )

    # print(common_names_df.show(truncate=False))

    pharmaceutical_names_df: DataFrame = (
        non_scientific_names_df.filter(f.col("name_type") == "pharmaceutical")
        .transform(transform_non_scientific_names)
        .transform(
            lambda df: construct_non_scientific_name_struct(df, "pharmaceutical_names")
        )
    )

    # print(pharmaceutical_names_df.show(truncate=False))

    plants_name_ids_only_df: DataFrame = plants_df.select(
        "name_id"
    ).dropDuplicates()  # Temporary dropDuplicates

    common_and_pharmaceutical_names_df: DataFrame = plants_name_ids_only_df.join(
        common_names_df,
        plants_name_ids_only_df.name_id == common_names_df.plant_id,
        how="left",
    ).drop("plant_id")

    common_and_pharmaceutical_names_df: DataFrame = (
        common_and_pharmaceutical_names_df.join(
            pharmaceutical_names_df,
            common_and_pharmaceutical_names_df.name_id
            == pharmaceutical_names_df.plant_id,
            how="left",
        )
        .drop(pharmaceutical_names_df.plant_id)
        .transform(add_non_scientific_name_counts)
    )

    # print(common_and_pharmaceutical_names_df.show(truncate=False))

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
    # Tidy up null counts with zeroes - caused by no data available - that's fine.
    all_name_mappings_df: DataFrame = all_name_mappings_df.withColumn(
        "mapping_id",
        f.row_number().over(Window.orderBy("scientific_name")),
    ).transform(replace_null_counts_with_zero)

    # print(all_name_mappings_df.show(truncate=False))

    # Write name mappings to JSON or parquet files
    write_name_mappings_to_file(
        df=all_name_mappings_df, output_filepath=output_filepath, sample_run=sample_run
    )

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


def transform_non_scientific_names(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("non_scientific_name", f.col("name"))
        .withColumn(
            "non_scientific_name_length", f.length(f.col("non_scientific_name"))
        )
        .withColumn("non_scientific_name_id", f.col("name_id"))
    )


def construct_non_scientific_name_struct(df: DataFrame, col_name: str) -> DataFrame:
    return df.groupBy("plant_id").agg(
        f.collect_list(
            f.struct(
                f.col("non_scientific_name"),
                f.col("non_scientific_name_id"),
                f.col("non_scientific_name_length"),
            )
        ).alias(col_name)
    )


def add_non_scientific_name_counts(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("common_name_count", f.size("common_names"))
        .withColumn("pharmaceutical_name_count", f.size("pharmaceutical_names"))
        .withColumn(
            "common_name_count",
            f.when(f.col("pharmaceutical_name_count") == f.lit(-1), 0).otherwise(
                f.col("common_name_count")
            ),
        )
        .withColumn(
            "pharmaceutical_name_count",
            f.when(f.col("pharmaceutical_name_count") == f.lit(-1), 0).otherwise(
                f.col("pharmaceutical_name_count")
            ),
        )
        .withColumn(
            "non_scientific_name_count",
            f.col("common_name_count") + f.col("pharmaceutical_name_count"),
        )
    )


def replace_null_counts_with_zero(df: DataFrame) -> DataFrame:
    return df.fillna(
        {
            "common_name_count": 0,
            "pharmaceutical_name_count": 0,
            "non_scientific_name_count": 0,
        }
    )


def create_plants_to_common_and_pharmaceutical_names_df(
    plants_df: DataFrame,
    common_and_pharmaceutical_names_df: DataFrame,
    exclude_quality_rating: List[str],
    exclude_taxon_status: List[str],
) -> DataFrame:
    # Filter plants DataFrame, join to common names & pharmaceutical names,
    # construct the name mappings from this.
    filtered_plants_df: DataFrame = (
        filter_exclusions(
            df=plants_df,
            exclude_quality_rating=exclude_quality_rating,
            exclude_taxon_status=exclude_taxon_status,
        )
        .withColumnRenamed("name_id", "scientific_name_id")
        .withColumnRenamed("full_scientific_name", "scientific_name")
        .withColumn("scientific_name_length", f.length(f.col("scientific_name")))
        .withColumn("scientific_name_type", f.lit("plant"))
    )

    plants_to_non_scientific_names_df: DataFrame = filtered_plants_df.join(
        common_and_pharmaceutical_names_df,
        filtered_plants_df.scientific_name_id
        == common_and_pharmaceutical_names_df.name_id,
        "left",
    )

    plants_to_non_scientific_names_cols: List = [
        "scientific_name_id",
        "scientific_name",
        "scientific_name_type",
        "scientific_name_length",
        "common_names",
        "pharmaceutical_names",
        "common_name_count",
        "pharmaceutical_name_count",
        "non_scientific_name_count",
    ]

    return plants_to_non_scientific_names_df.select(
        *plants_to_non_scientific_names_cols
    )


def create_synonyms_to_common_and_pharmaceutical_names_df(
    synonyms_df: DataFrame,
    common_and_pharmaceutical_names_df: DataFrame,
    exclude_quality_rating: List[str],
    exclude_taxon_status: List[str],
) -> DataFrame:
    # Filter synonyms DataFrame, join to common names & pharmaceutical names,
    # construct the name mappings from this.
    filtered_synonyms_df: DataFrame = (
        filter_exclusions(
            df=synonyms_df,
            exclude_quality_rating=exclude_quality_rating,
            exclude_taxon_status=exclude_taxon_status,
        )
        .withColumnRenamed("acc_name_id", "scientific_name_id")
        .withColumnRenamed("full_scientific_name", "scientific_name")
        .withColumn("scientific_name_length", f.length(f.col("scientific_name")))
        .withColumn("scientific_name_type", f.lit("synonym"))
    )

    synonyms_to_non_scientific_names_df: DataFrame = filtered_synonyms_df.join(
        common_and_pharmaceutical_names_df,
        filtered_synonyms_df.scientific_name_id
        == common_and_pharmaceutical_names_df.name_id,
        "left",
    )

    synonyms_to_non_scientific_names_cols: List = [
        "scientific_name_id",
        "scientific_name",
        "scientific_name_type",
        "scientific_name_length",
        "common_names",
        "pharmaceutical_names",
        "common_name_count",
        "pharmaceutical_name_count",
        "non_scientific_name_count",
    ]

    return synonyms_to_non_scientific_names_df.select(
        *synonyms_to_non_scientific_names_cols
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

    # Join sci_cited_medicinal_names to common names & pharmaceutical names.
    # Note that the plant_id here will be a full_scientific_name_id,
    # or the name_id that matches an entry in the plants table.
    # So the plant_id is joined to plant_id here.
    # The rename here are just to make this consistent to use
    # the construct_name_mappings_df() function. The order is important.
    sci_cited_medicinal_names_df: DataFrame = (
        sci_cited_medicinal_names_df.withColumnRenamed("name_id", "scientific_name_id")
        .withColumnRenamed("name", "scientific_name")
        .drop("name_type")
        .withColumn("scientific_name_length", f.length(f.col("scientific_name")))
        .withColumn("scientific_name_type", f.lit("sci_cited_medicinal"))
    )

    sci_cited_medicinal_to_non_scientific_names_df: DataFrame = (
        sci_cited_medicinal_names_df.join(
            common_and_pharmaceutical_names_df,
            sci_cited_medicinal_names_df.scientific_name_id
            == common_and_pharmaceutical_names_df.name_id,
            "left",
        )
    )

    sci_cited_medicinal_to_non_scientific_names_cols: List = [
        "scientific_name_id",
        "scientific_name",
        "scientific_name_type",
        "scientific_name_length",
        "common_names",
        "pharmaceutical_names",
        "common_name_count",
        "pharmaceutical_name_count",
        "non_scientific_name_count",
    ]

    return sci_cited_medicinal_to_non_scientific_names_df.select(
        *sci_cited_medicinal_to_non_scientific_names_cols
    )


def write_name_mappings_to_file(
    df: DataFrame, output_filepath: str, sample_run: bool
) -> None:
    output_filepath_parent: Path = Path(output_filepath).parents[0]
    output_filepath_parent.mkdir(parents=True, exist_ok=True)
    if sample_run:
        # Coalesce to 1 JSON file for sample demonstration
        df.coalesce(1).write.format("json").mode("overwrite").option(
            "schema", OUTPUT_SCHEMA_V5
        ).save(output_filepath)

    else:
        # Repartition to ballpark of 5 parquet files for real data
        df.repartition(5).write.mode("overwrite").option(
            "schema", OUTPUT_SCHEMA_V5
        ).partitionBy("scientific_name_type").parquet(output_filepath)


def write_process_metadata(df: DataFrame, output_filepath: Path) -> None:
    total_count: int = df.count()
    plant_name_count: int = df.filter(f.col("scientific_name_type") == "plant").count()
    synonym_name_count: int = df.filter(
        f.col("scientific_name_type") == "synonym"
    ).count()
    sci_cited_medicinal_name_count: int = df.filter(
        f.col("scientific_name_type") == "sci_cited_medicinal"
    ).count()

    _metadata = {
        "total_count": total_count,
        "plant_name_count": plant_name_count,
        "synonym_name_count": synonym_name_count,
        "sci_cited_medicinal_name_count": sci_cited_medicinal_name_count,
    }

    with Path(f"{output_filepath}/ process_metadata.json").open(
        "w", encoding="utf-8"
    ) as file:
        json.dump(_metadata, file)


def check_if_contains_only_allowed_characters(target: str) -> bool:
    allowed_chars = (
        list(string.ascii_lowercase[:])
        + list(string.ascii_uppercase[:])
        + [".", " ", "(", ")", "'", "-"]
    )
    return all(t in allowed_chars for t in target)


check_if_contains_only_allowed_characters_udf = f.udf(
    check_if_contains_only_allowed_characters, BooleanType()
)

# TODO: Use argparse to pass sample_run as a flag to container.

# These are here for demonstration purposes
sample_mpns_raw_filepath = "data/mpns/sample_mpns_v8/"
sample_mpns_processed_filepath = (
    "data/processed/mpns/sample_mpns_v8/mpns_name_mappings/v5/"
)
process_mpns_v8_raw(
    input_filepath=sample_mpns_raw_filepath,
    output_filepath=sample_mpns_processed_filepath,
    exclude_quality_rating=["L"],
    exclude_taxon_status=["Misapplied"],
    sample_run=True,
)

# mpns_raw_filepath = "data/mpns/mpns_v8/"
# mpns_processed_filepath = "data/processed/mpns/mpns_v8/mpns_name_mappings/v5/"
# process_mpns_v8_raw(
#     input_filepath=mpns_raw_filepath,
#     output_filepath=mpns_processed_filepath,
#     exclude_quality_rating=["L"],
#     exclude_taxon_status=["Misapplied"],
#     sample_run=False,
# )
