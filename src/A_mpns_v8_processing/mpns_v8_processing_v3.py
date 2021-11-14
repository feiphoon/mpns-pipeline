import os
from functools import reduce
from pathlib import Path

from pyspark.sql import SparkSession, functions as f
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType

from pyspark.sql.window import Window

from input_schemas import (
    MPNS_V8_PLANTS,
    MPNS_V8_SYNONYMS,
    MPNS_V8_NON_SCIENTIFIC_NAMES,
)


def process_mpns_v8_raw(
    input_filepath: Path,
    # output_filepath: Path,
    exclude_quality_rating: List[str],
    exclude_taxon_status: List[str],
    # sample_run: bool,
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

    # Group name mappings
    plants_to_common_and_pharmaceutical_names_df = (
        plants_to_common_and_pharmaceutical_names_df.transform(group_name_mappings)
    )

    synonyms_to_common_and_pharmaceutical_names_df = (
        synonyms_to_common_and_pharmaceutical_names_df.transform(group_name_mappings)
    )

    sci_cited_medicinal_to_common_and_pharmaceutical_names_df = (
        sci_cited_medicinal_to_common_and_pharmaceutical_names_df.transform(
            group_name_mappings
        )
    )

    # Join all three name mappings DataFrames for everything
    _dfs_to_union: list = [
        plants_to_common_and_pharmaceutical_names_df,
        synonyms_to_common_and_pharmaceutical_names_df,
        sci_cited_medicinal_to_common_and_pharmaceutical_names_df,
    ]

    all_name_mappings_df: DataFrame = reduce(DataFrame.union, _dfs_to_union)


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


def group_name_mappings(df: DataFrame) -> DataFrame:
    grouped_df: DataFrame = (
        df.withColumn(
            "non_scientific_names",
            f.struct(
                "non_scientific_name_type",
                "non_scientific_name",
                "non_scientific_name_id",
            ),
        )
        .groupBy("scientific_name")
        .agg(
            f.collect_list(f.col("non_scientific_names")).alias("non_scientific_names")
        )
        .orderBy("scientific_name")
    )

    schema: list = [
        "scientific_name",
        "scientific_name_id",
        "scientific_name_type",
        "non_scientific_names",
    ]

    df = df.drop(
        "non_scientific_name", "non_scientific_name_id", "non_scientific_name_type"
    )

    return (
        grouped_df.join(
            df, on=(grouped_df.scientific_name == df.scientific_name), how="left"
        )
        .drop(df.scientific_name)
        .select(*schema)
    )
