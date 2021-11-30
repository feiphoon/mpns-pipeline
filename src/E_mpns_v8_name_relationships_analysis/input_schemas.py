from pyspark.sql.types import StructType, StructField, StringType


# Raw input schemas
MPNS_V8_PLANTS = StructType(
    [
        StructField("name_id", StringType(), True),
        StructField("ipni_id", StringType(), True),
        StructField("taxon_status", StringType(), True),
        StructField("quality_rating", StringType(), True),
        StructField("rank", StringType(), True),
        StructField("family", StringType(), True),
        StructField("genus", StringType(), True),
        StructField("genus_hybrid", StringType(), True),
        StructField("species", StringType(), True),
        StructField("species_hybrid", StringType(), True),
        StructField("infra_species", StringType(), True),
        StructField("parent_author", StringType(), True),
        StructField("primary_author", StringType(), True),
        StructField("full_scientific_name", StringType(), True),
    ]
)

MPNS_V8_SYNONYMS = StructType(
    [
        StructField("name_id", StringType(), True),
        StructField("ipni_id", StringType(), True),
        StructField("taxon_status", StringType(), True),
        StructField("quality_rating", StringType(), True),
        StructField("rank", StringType(), True),
        StructField("genus", StringType(), True),
        StructField("genus_hybrid", StringType(), True),
        StructField("species", StringType(), True),
        StructField("species_hybrid", StringType(), True),
        StructField("infra_species", StringType(), True),
        StructField("parent_author", StringType(), True),
        StructField("primary_author", StringType(), True),
        StructField("full_scientific_name", StringType(), True),
        StructField("acc_name_id", StringType(), True),
    ]
)

MPNS_V8_NON_SCIENTIFIC_NAMES = StructType(
    [
        StructField("name_type", StringType(), True),
        StructField("name", StringType(), True),
        StructField("plant_id", StringType(), True),
        StructField("name_id", StringType(), True),
    ]
)
