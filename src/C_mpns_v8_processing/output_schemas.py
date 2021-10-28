from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
)

OUTPUT_SCHEMA_V1 = StructType(
    [
        StructField("mapping_id", IntegerType(), False),
        StructField("full_scientific_name", StringType(), False),
        StructField("full_scientific_name_id", StringType(), False),
        StructField("is_synonym", BooleanType(), False),
        StructField("non_scientific_name", StringType(), False),
        StructField("non_scientific_type", StringType(), False),
    ]
)

OUTPUT_SCHEMA_V2 = StructType(
    [
        StructField("mapping_id", IntegerType(), False),
        StructField("full_scientific_name_id", StringType(), False),
        StructField("scientific_name", StringType(), False),
        StructField("scientific_name_type", StringType(), False),
        StructField("non_scientific_name", StringType(), False),
        StructField("non_scientific_type", StringType(), False),
    ]
)

# PROCESS_METADATA_SCHEMA = StructType(
#     [
#         StructField("total_count", IntegerType(), False),
#         StructField("is_synonym_count", IntegerType(), False),
#         StructField("is_not_synonym_count", IntegerType(), False),
#         StructField("is_common_name_count", IntegerType(), False),
#         StructField("is_pharmaceutical_name_count", IntegerType(), False),
#         StructField("is_sci_cited_medicinal_name_count", IntegerType(), False),
#     ]
# )
