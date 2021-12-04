from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
)

OUTPUT_SCHEMA = StructType(
    [
        StructField("plant_id", StringType(), False),
        StructField("plant_full_scientific_name", StringType(), False),
        StructField(
            "synonym_full_scientific_name", ArrayType(StringType(), False), False
        ),
        StructField("synonym_count", IntegerType(), False),
        StructField(
            "non_scientific_name_pairs",
            ArrayType(
                StructType(
                    [
                        StructField("non_scientific_name", StringType(), False),
                        StructField("non_scientific_name_type", StringType(), False),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField("total_non_scientific_name_count", IntegerType(), False),
        StructField(
            "scm_com_pha",
            StructType(
                [
                    StructField("scm", IntegerType(), False),
                    StructField("com", IntegerType(), False),
                    StructField("pha", IntegerType(), False),
                ]
            ),
            False,
        ),
        StructField("scm_non_scientific_name_count", IntegerType(), False),
        StructField("com_non_scientific_name_count", IntegerType(), False),
        StructField("pha_non_scientific_name_count", IntegerType(), False),
    ]
)
