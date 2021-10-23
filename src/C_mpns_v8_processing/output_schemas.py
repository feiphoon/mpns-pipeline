from pyspark.sql.types import (
    StructType,
    StructField,
    ArrayType,
    StringType,
    IntegerType,
)

OUTPUT_SCHEMA = StructType(
    [
        StructField("mapping_id", StringType(), False),
        StructField("full_scientific_name", StringType(), False),
        StructField("mpns_id", StringType(), False),
        StructField("is_synonym", StringType(), False),
        StructField("non_scientific_names_total", IntegerType(), False),
        StructField(
            "non_scientific_names",
            ArrayType(
                StructType(
                    [
                        StructField("name", StringType(), False),
                        StructField("type", StringType(), False),
                    ]
                ),
                True,
            ),
            False,
        ),
    ]
)
