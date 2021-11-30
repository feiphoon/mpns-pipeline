from pathlib import Path

from pyspark.sql import SparkSession, functions as f
from pyspark.sql.dataframe import DataFrame


#  Monkeypatch in case I don't use Spark 3.0
def transform(self, f):
    return f(self)


DataFrame.transform = transform


def query_mpns_v8_name_relationships(
    input_filepath: str,
    output_filepath: str,
    sample_run: bool,
) -> None:
    spark = SparkSession.builder.appName(
        "query_mpns_v8_name_relationships"
    ).getOrCreate()

    if sample_run:
        name_relationships_df: DataFrame = spark.read.json(input_filepath)
    else:
        name_relationships_df: DataFrame = spark.read.parquet(input_filepath)

    plants_with_minimum_relationships_df: DataFrame = name_relationships_df.transform(
        lambda df: get_plants_with_minimum_relationships(df)
    )

    write_query_results(plants_with_minimum_relationships_df, output_filepath)


def write_query_results(df: DataFrame, output_filepath: Path) -> None:
    output_filepath_parent: Path = Path(output_filepath).parents[0]
    output_filepath_parent.mkdir(parents=True, exist_ok=True)

    # Coalesce to 1 JSON file
    df.coalesce(1).write.format("json").mode("overwrite").save(output_filepath)


def get_plants_with_minimum_relationships(df: DataFrame) -> DataFrame:
    df = df.filter(
        (f.col("synonym_count") >= 1)
        & (f.col("scm_non_scientific_name_count") >= 1)
        & (f.col("com_non_scientific_name_count") >= 1)
        & (f.col("pha_non_scientific_name_count") >= 1)
    )
    return df.sort(df.synonym_count.asc(), df.scm_com_pha.asc())


# Sample/demo purposes
mpns_v8_name_relationships_filepath: str = (
    "data/analysis/mpns/sample_mpns_v8/name_relationships/"
)
output_filepath: str = "data/analysis/mpns/sample_mpns_v8/name_relationships/query/"
query_mpns_v8_name_relationships(
    input_filepath=mpns_v8_name_relationships_filepath,
    output_filepath=output_filepath,
    sample_run=True,
)


# # Real data
# mpns_v8_name_relationships_filepath: str = (
#     "data/analysis/mpns/mpns_v8/name_relationships/"
# )
# output_filepath: str = "data/analysis/mpns/mpns_v8/name_relationships/query/"
# query_mpns_v8_name_relationships(
#     input_filepath=mpns_v8_name_relationships_filepath,
#     output_filepath=output_filepath,
#     sample_run=False,
# )
