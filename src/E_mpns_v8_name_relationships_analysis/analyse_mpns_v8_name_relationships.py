import os
from pathlib import Path
from typing import List, Tuple

from pyspark.sql import SparkSession, functions as f
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType

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


def analyse_mpns_v8_name_relationships(
    input_filepath: str,
    output_filepath: str,
    exclude_quality_rating: List[str],
    exclude_taxon_status: List[str],
    sample_run: bool,
) -> None:
    spark = SparkSession.builder.appName(
        "mpns_v8_name_relationships_analysis"
    ).getOrCreate()

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

    # Filter plants and synonyms datasets on quality_rating and taxon_status
    # Select relevant columns
    filtered_plants_df: DataFrame = (
        filter_exclusions(
            df=plants_df,
            exclude_quality_rating=exclude_quality_rating,
            exclude_taxon_status=exclude_taxon_status,
        )
        .select("name_id", "full_scientific_name")
        .withColumnRenamed("name_id", "plant_id")
        .withColumnRenamed("full_scientific_name", "plant_full_scientific_name")
    )

    filtered_synonyms_df: DataFrame = (
        filter_exclusions(
            df=synonyms_df,
            exclude_quality_rating=exclude_quality_rating,
            exclude_taxon_status=exclude_taxon_status,
        )
        .select("acc_name_id", "full_scientific_name")
        .withColumnRenamed("acc_name_id", "synonym_id")
        .withColumnRenamed("full_scientific_name", "synonym_full_scientific_name")
    )

    non_scientific_names_df = (
        non_scientific_names_df.select("plant_id", "name", "name_type")
        .withColumnRenamed("plant_id", "non_scientific_name_id")
        .withColumnRenamed("name", "non_scientific_name")
        .withColumnRenamed("name_type", "non_scientific_name_type")
    )

    # print(filtered_plants_df.show(2))

    # Group synonyms by synonym_id.
    # Produce list of synonyms and produce count of this list
    filtered_synonyms_df = (
        filtered_synonyms_df.groupBy("synonym_id")
        .agg(
            f.collect_list("synonym_full_scientific_name").alias(
                "synonym_full_scientific_name"
            )
        )
        .withColumn("synonym_count", f.size("synonym_full_scientific_name"))
    )

    # print(filtered_synonyms_df.show(2, truncate=False))

    # Collapse non_scientific_names to (non_scientific_name, non_scientific_name_type)
    # Group non_scientific_name pairs by non_scientific_name_id
    # Produce list of non-scientific_names and produce counts of this list
    non_scientific_names_df = (
        non_scientific_names_df.withColumn(
            "non_scientific_name_pairs",
            f.struct(f.col("non_scientific_name"), f.col("non_scientific_name_type")),
        )
        .drop("non_scientific_name", "non_scientific_name_type")
        .groupBy("non_scientific_name_id")
        .agg(
            f.collect_list("non_scientific_name_pairs").alias(
                "non_scientific_name_pairs"
            )
        )
        .withColumn(
            "total_non_scientific_name_count", f.size("non_scientific_name_pairs")
        )
        .withColumn(
            "non_scientific_name_breakdown",
            f.col("non_scientific_name_pairs.non_scientific_name_type"),
        )
        .withColumn(
            "scm_com_pha",
            create_tuple_of_name_counts_udf(f.col("non_scientific_name_breakdown")),
        )
        .withColumn("scm_non_scientific_name_count", f.col("scm_com_pha.scm"))
        .withColumn("com_non_scientific_name_count", f.col("scm_com_pha.com"))
        .withColumn("pha_non_scientific_name_count", f.col("scm_com_pha.pha"))
    )

    # print(non_scientific_names_df.show(2, truncate=False))

    # Join all three dataframes for all information
    all_information_df: DataFrame = (
        filtered_plants_df.join(
            filtered_synonyms_df,
            filtered_plants_df.plant_id == filtered_synonyms_df.synonym_id,
            "left",
        )
        .join(
            non_scientific_names_df,
            filtered_plants_df.plant_id
            == non_scientific_names_df.non_scientific_name_id,
            "left",
        )
        .drop("synonym_id", "non_scientific_name_id")
    )

    # print(all_information_df.show(truncate=False))

    write_analysis_to_file(all_information_df, output_filepath, sample_run)


def write_analysis_to_file(
    df: DataFrame, output_filepath: str, sample_run: bool
) -> None:
    output_filepath_parent: Path = Path(output_filepath).parents[0]
    output_filepath_parent.mkdir(parents=True, exist_ok=True)
    if sample_run:
        # Coalesce to 1 JSON file for sample demonstration
        df.coalesce(1).write.format("json").mode("overwrite").option(
            "schema", OUTPUT_SCHEMA
        ).save(output_filepath)

    else:
        # Coalesce to 1 parquet file
        df.coalesce(1).write.mode("overwrite").option("schema", OUTPUT_SCHEMA).parquet(
            output_filepath
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


def create_tuple_of_name_counts(struct: List[str]) -> Tuple[int, int, int]:
    return tuple(
        [
            struct.count("sci_cited_medicinal"),
            struct.count("common"),
            struct.count("pharmaceutical"),
        ]
    )


create_tuple_of_name_counts_udf = f.udf(
    create_tuple_of_name_counts,
    StructType(
        [
            StructField("scm", IntegerType(), False),
            StructField("com", IntegerType(), False),
            StructField("pha", IntegerType(), False),
        ]
    ),
)


# Sample/demo purposes
sample_mpns_raw_filepath: str = "data/mpns/sample_mpns_v8/"
output_filepath: str = "data/analysis/mpns/sample_mpns_v8/name_relationships/"

analyse_mpns_v8_name_relationships(
    input_filepath=sample_mpns_raw_filepath,
    output_filepath=output_filepath,
    exclude_quality_rating=["L"],
    exclude_taxon_status=["Misapplied"],
    sample_run=True,
)

# # Real run with real data
# mpns_raw_filepath: str = "data/mpns/mpns_v8/"
# output_filepath: str = "data/analysis/mpns/mpns_v8/name_relationships/"

# analyse_mpns_v8_name_relationships(
#     input_filepath=mpns_raw_filepath,
#     output_filepath=output_filepath,
#     exclude_quality_rating=["L"],
#     exclude_taxon_status=["Misapplied"],
#     sample_run=False,
# )
