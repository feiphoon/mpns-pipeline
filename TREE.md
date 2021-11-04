# File tree

This file explains the different folders and files in this repo.


## Code:
```
src
├── A_mpns_v8_processing
│   ├── README.md
│   ├── input_schemas.py
│   ├── mpns_v8_processing_v1.py
│   ├── mpns_v8_processing_v2.py
│   └── output_schemas.py
├── B_stratify_mpns_name_mappings
│   └── README.md
├── C_create_labels_for_annotation
│   └── README.md
└── __init_.py
```


## Data:

### Directories only

```
data
├── mpns
│   ├── mpns_v8
│   └── sample_mpns_v8
├── processed
│   └── mpns
│       ├── mpns_v8
│       │   └── mpns_name_mappings
│       │       ├── v1
│       │       ├── v2
│       │       │   ├── scientific_name_type=plant
│       │       │   ├── scientific_name_type=sci_cited_medicinal
│       │       │   └── scientific_name_type=synonym
│       │       └── v2_non_partitioned
│       └── sample_mpns_v8
│           └── mpns_name_mappings
│               ├── v1
│               └── v2
└── reference
    └── mpns_v8
```

### Directories and contents
```
data
├── mpns
│   ├── mpns_v8
│   │   ├── medicinal_mpns_non_scientific_names.csv
│   │   ├── medicinal_mpns_plants.csv
│   │   └── medicinal_mpns_synonyms.csv
│   └── sample_mpns_v8
│       ├── medicinal_mpns_non_scientific_names.csv
│       ├── medicinal_mpns_plants.csv
│       └── medicinal_mpns_synonyms.csv
├── processed
│   └── mpns
│       ├── mpns_v8
│       │   └── mpns_name_mappings
│       │       ├── v1
│       │       │   ├── \ process_metadata.json
│       │       │   ├── _SUCCESS
│       │       │   ├── part-00000-037e035e-947e-4d19-82e7-f5a41c5dc82a-c000.json.gz
│       │       │   ├── part-00001-037e035e-947e-4d19-82e7-f5a41c5dc82a-c000.json.gz
│       │       │   ├── part-00002-037e035e-947e-4d19-82e7-f5a41c5dc82a-c000.json.gz
│       │       │   ├── part-00003-037e035e-947e-4d19-82e7-f5a41c5dc82a-c000.json.gz
│       │       │   └── part-00004-037e035e-947e-4d19-82e7-f5a41c5dc82a-c000.json.gz
│       │       ├── v2
│       │       │   ├── \ process_metadata.json
│       │       │   ├── _SUCCESS
│       │       │   ├── scientific_name_type=plant
│       │       │   │   ├── part-00000-9d758e65-504b-4705-b466-b3b1accda5b3.c000.snappy.parquet
│       │       │   │   ├── part-00001-9d758e65-504b-4705-b466-b3b1accda5b3.c000.snappy.parquet
│       │       │   │   ├── part-00002-9d758e65-504b-4705-b466-b3b1accda5b3.c000.snappy.parquet
│       │       │   │   ├── part-00003-9d758e65-504b-4705-b466-b3b1accda5b3.c000.snappy.parquet
│       │       │   │   └── part-00004-9d758e65-504b-4705-b466-b3b1accda5b3.c000.snappy.parquet
│       │       │   ├── scientific_name_type=sci_cited_medicinal
│       │       │   │   ├── part-00000-9d758e65-504b-4705-b466-b3b1accda5b3.c000.snappy.parquet
│       │       │   │   ├── part-00001-9d758e65-504b-4705-b466-b3b1accda5b3.c000.snappy.parquet
│       │       │   │   ├── part-00002-9d758e65-504b-4705-b466-b3b1accda5b3.c000.snappy.parquet
│       │       │   │   ├── part-00003-9d758e65-504b-4705-b466-b3b1accda5b3.c000.snappy.parquet
│       │       │   │   └── part-00004-9d758e65-504b-4705-b466-b3b1accda5b3.c000.snappy.parquet
│       │       │   └── scientific_name_type=synonym
│       │       │       ├── part-00000-9d758e65-504b-4705-b466-b3b1accda5b3.c000.snappy.parquet
│       │       │       ├── part-00001-9d758e65-504b-4705-b466-b3b1accda5b3.c000.snappy.parquet
│       │       │       ├── part-00002-9d758e65-504b-4705-b466-b3b1accda5b3.c000.snappy.parquet
│       │       │       ├── part-00003-9d758e65-504b-4705-b466-b3b1accda5b3.c000.snappy.parquet
│       │       │       └── part-00004-9d758e65-504b-4705-b466-b3b1accda5b3.c000.snappy.parquet
│       │       └── v2_non_partitioned
│       │           ├── \ process_metadata.json
│       │           ├── _SUCCESS
│       │           ├── part-00000-61712b09-deaf-4154-b58b-78f44bcd2f0d-c000.snappy.parquet
│       │           ├── part-00001-61712b09-deaf-4154-b58b-78f44bcd2f0d-c000.snappy.parquet
│       │           ├── part-00002-61712b09-deaf-4154-b58b-78f44bcd2f0d-c000.snappy.parquet
│       │           ├── part-00003-61712b09-deaf-4154-b58b-78f44bcd2f0d-c000.snappy.parquet
│       │           └── part-00004-61712b09-deaf-4154-b58b-78f44bcd2f0d-c000.snappy.parquet
│       └── sample_mpns_v8
│           └── mpns_name_mappings
│               ├── v1
│               │   ├── \ process_metadata.json
│               │   ├── _SUCCESS
│               │   └── part-00000-e465cfb7-f1d6-4105-bb52-dec50e24879b-c000.json
│               └── v2
│                   ├── \ process_metadata.json
│                   ├── _SUCCESS
│                   └── part-00000-e4899cf1-e5a8-4b87-8702-fca302ab8442-c000.json
└── reference
    └── mpns_v8
        └── annotation_labels.json
```