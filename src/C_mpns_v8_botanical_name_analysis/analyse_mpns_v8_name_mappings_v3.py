"""
Note that a processing v4 has since been made, but this analysis method is still correct. This is because v3 grouped the
mappings, and v4 contains exploded mappings. Doing this analysis on v3 just saves us doing a distinct operation on the
scientific names.
"""
import json

from pathlib import Path

from pyspark.sql import SparkSession, functions as f
from pyspark.sql.dataframe import DataFrame


from botanical_name_regexes import BotanicalNameRegexes


#  Monkeypatch in case I don't use Spark 3.0
def transform(self, f):
    return f(self)


DataFrame.transform = transform


def analyse_mpns_v8_name_mappings_v3(
    name_mappings_filepath: Path,
    metadata_output_filepath: Path,
) -> None:
    spark = SparkSession.builder.appName(
        "mpns_v8_name_mappings_v3_analysis"
    ).getOrCreate()

    name_mappings_df: DataFrame = spark.read.option(
        "basePath", f"{name_mappings_filepath}"
    ).load(f"{name_mappings_filepath}/scientific_name_type=*/")

    # Count the different name types.
    # Note that there will be some overlap, for example with Species name type.
    species_name_type_count: int = name_mappings_df.transform(
        lambda df: count_botanical_name_type(
            df, BotanicalNameRegexes.SPECIES_REGEX.value
        )
    )

    species_aggregate_name_type_count: int = name_mappings_df.transform(
        lambda df: count_botanical_name_type(
            df, BotanicalNameRegexes.SPECIES_AGGREGATE_REGEX.value
        )
    )

    intergeneric_hybrid_name_type_count: int = name_mappings_df.transform(
        lambda df: count_botanical_name_type(
            df, BotanicalNameRegexes.INTERGENERIC_HYBRID_REGEX.value
        )
    )

    interspecific_hybrid_name_type_count: int = name_mappings_df.transform(
        lambda df: count_botanical_name_type(
            df, BotanicalNameRegexes.INTERSPECIFIC_HYBRID_REGEX.value
        )
    )

    subspecies_name_type_count: int = name_mappings_df.transform(
        lambda df: count_botanical_name_type(
            df, BotanicalNameRegexes.SUBSPECIES_REGEX.value
        )
    )

    botanical_variety_name_type_count: int = name_mappings_df.transform(
        lambda df: count_botanical_name_type(
            df, BotanicalNameRegexes.BOTANICAL_VARIETY_REGEX.value
        )
    )

    cultivar_group_name_type_count: int = name_mappings_df.transform(
        lambda df: count_botanical_name_type(
            df, BotanicalNameRegexes.CULTIVAR_GROUP_REGEX.value
        )
    )

    cultivar_name_type_count: int = name_mappings_df.transform(
        lambda df: count_botanical_name_type(
            df, BotanicalNameRegexes.CULTIVAR_REGEX.value
        )
    )

    total_name_mappings_count: int = name_mappings_df.count()

    write_process_metadata(
        species=species_name_type_count,
        species_aggregate=species_aggregate_name_type_count,
        intergeneric_hybrid_name=intergeneric_hybrid_name_type_count,
        interspecific_hybrid=interspecific_hybrid_name_type_count,
        subspecies=subspecies_name_type_count,
        botanical_variety=botanical_variety_name_type_count,
        cultivar_group=cultivar_group_name_type_count,
        cultivar=cultivar_name_type_count,
        total_name_mappings=total_name_mappings_count,
        metadata_output_filepath=metadata_output_filepath,
    )


def count_botanical_name_type(df: DataFrame, pattern: BotanicalNameRegexes) -> int:
    """Returns statistics for matches of botanical name regexes in the scientific name of a mapping."""
    scientific_name_df: DataFrame = df.select(f.col("scientific_name")).withColumn(
        "matched",
        f.when(f.col("scientific_name").rlike(pattern), True).otherwise(False),
    )

    return scientific_name_df.filter(f.col("matched")).count()


def write_process_metadata(
    species: int,
    species_aggregate: int,
    intergeneric_hybrid_name: int,
    interspecific_hybrid: int,
    subspecies: int,
    botanical_variety: int,
    cultivar_group: int,
    cultivar: int,
    total_name_mappings: int,
    metadata_output_filepath: Path,
) -> None:
    _metadata = {
        "species_name_type_count": species,
        "species_aggregate_name_type_count": species_aggregate,
        "intergeneric_hybrid_name_type_count": intergeneric_hybrid_name,
        "interspecific_hybrid_name_type_count": interspecific_hybrid,
        "subspecies_name_type_count": subspecies,
        "botanical_variety_name_type_count": botanical_variety,
        "cultivar_group_name_type_count": cultivar_group,
        "cultivar_name_type_count": cultivar,
        "total_name_mappings_count": total_name_mappings,
    }

    with Path(
        f"{metadata_output_filepath}/name_mappings_v3_botanical_name_type_counts.json"
    ).open("w", encoding="utf-8") as file:
        json.dump(_metadata, file)


name_mappings_filepath: Path = Path(
    "data/processed/mpns/mpns_v8/mpns_name_mappings/v3/"
)
metadata_output_filepath: Path = Path("data/analysis/mpns/mpns_v8/botanical_names/")
metadata_output_filepath.mkdir(parents=True, exist_ok=True)


analyse_mpns_v8_name_mappings_v3(
    name_mappings_filepath=name_mappings_filepath,
    metadata_output_filepath=metadata_output_filepath,
)
