from pyspark.sql.types import (
    StructType,
    StructField,
    ArrayType,
    StringType,
    IntegerType,
    BooleanType,
)

OUTPUT_SCHEMA = StructType(
    [
        StructField("mapping_id", IntegerType(), False),
        StructField("full_scientific_name", StringType(), False),
        StructField("full_scientific_name_id", StringType(), False),
        StructField("is_synonym", BooleanType(), False),
        StructField("non_scientific_name", StringType(), False),
        StructField("non_scientific_type", StringType(), False),
    ]
)

PROCESS_METADATA_SCHEMA = StructType(
    [
        StructField(
            "file_types",
            ArrayType(
                StructType(
                    [
                        StructField("dataset_name", StringType(), False),
                        StructField("initial_row_count", StringType(), False),
                        StructField("end_row_count", StringType(), False),
                    ]
                ),
                True,
            ),
            False,
        ),
    ]
)
