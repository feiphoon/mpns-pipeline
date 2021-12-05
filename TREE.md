# File tree

This file explains the different folders and files in this repo.

- [File tree](#file-tree)
  * [Code](#code)
  * [Tests](#tests)
  * [Data](#data)
  * [All](#all)


## Code

```bash
tree -L 3
```

```
.
├── A_mpns_v8_processing
│   ├── README.md
│   ├── input_schemas.py
│   ├── mpns_v8_processing_v1.py
│   ├── mpns_v8_processing_v2.py
│   ├── mpns_v8_processing_v3.py
│   ├── mpns_v8_processing_v4.py
│   ├── mpns_v8_processing_v5.py
│   └── output_schemas.py
├── B_create_labels_for_annotation
│   └── README.md
├── C_mpns_v8_botanical_name_analysis
│   ├── README.md
│   ├── analyse_mpns_v8_name_mappings_v3.py
│   └── botanical_name_regexes.py
├── D_mpns_v8_name_relationships_analysis
│   ├── README.md
│   ├── analyse_mpns_v8_name_relationships.py
│   ├── input_schemas.py
│   └── output_schemas.py
└── E_get_mpns_v8_analyses_answers
    ├── README.md
    └── query_mpns_v8_name_relationships.py

```

## Tests

```bash
tree -L 2
```

```
.
├── test_botanical_name_regexes.cpython-39-pytest-6.2.5.pyc
└── helpers
    └── test_botanical_name_regexes.py
```




## Data

```bash
tree -L 6
```
```
.
├── analysis
│   └── mpns
│       ├── mpns_v8
│       │   ├── botanical_names
│       │   │   └── name_mappings_v3_botanical_name_type_counts.json
│       │   └── name_relationships
│       │       ├── _SUCCESS
│       │       ├── part-00000-154f8e08-6d89-47be-a53f-7314d2846a45-c000.snappy.parquet
│       │       └── query
│       │           ├── _SUCCESS
│       │           └── part-00000-cf29ee2b-89ec-49c3-b966-d200661651e5-c000.json
│       └── sample_mpns_v8
│           └── name_relationships
│               ├── _SUCCESS
│               ├── part-00000-f24ff1dc-e046-4a26-9c0d-bbf10c28aa14-c000.json
│               └── query
│                   ├── _SUCCESS
│                   └── part-00000-785016e9-b0b3-482d-92cc-297783a37b7b-c000.json
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
│       │       │   ├── scientific_name_type=sci_cited_medicinal
│       │       │   └── scientific_name_type=synonym
│       │       ├── v2_non_partitioned
│       │       │   ├── \ process_metadata.json
│       │       │   ├── _SUCCESS
│       │       │   ├── part-00000-61712b09-deaf-4154-b58b-78f44bcd2f0d-c000.snappy.parquet
│       │       │   ├── part-00001-61712b09-deaf-4154-b58b-78f44bcd2f0d-c000.snappy.parquet
│       │       │   ├── part-00002-61712b09-deaf-4154-b58b-78f44bcd2f0d-c000.snappy.parquet
│       │       │   ├── part-00003-61712b09-deaf-4154-b58b-78f44bcd2f0d-c000.snappy.parquet
│       │       │   └── part-00004-61712b09-deaf-4154-b58b-78f44bcd2f0d-c000.snappy.parquet
│       │       ├── v3
│       │       │   ├── _SUCCESS
│       │       │   ├── process_metadata.json
│       │       │   ├── scientific_name_type=__HIVE_DEFAULT_PARTITION__
│       │       │   ├── scientific_name_type=plant
│       │       │   ├── scientific_name_type=sci_cited_medicinal
│       │       │   └── scientific_name_type=synonym
│       │       ├── v4
│       │       │   ├── \ process_metadata.json
│       │       │   ├── _SUCCESS
│       │       │   ├── scientific_name_type=plant
│       │       │   ├── scientific_name_type=sci_cited_medicinal
│       │       │   └── scientific_name_type=synonym
│       │       └── v5
│       │           ├── \ process_metadata.json
│       │           ├── _SUCCESS
│       │           ├── scientific_name_type=plant
│       │           ├── scientific_name_type=sci_cited_medicinal
│       │           └── scientific_name_type=synonym
│       └── sample_mpns_v8
│           └── mpns_name_mappings
│               ├── test
│               │   ├── sample_intermediate_names_v3.json
│               │   └── sample_names_v3.json
│               ├── v1
│               │   ├── \ process_metadata.json
│               │   ├── _SUCCESS
│               │   └── part-00000-e465cfb7-f1d6-4105-bb52-dec50e24879b-c000.json
│               ├── v2
│               │   ├── \ process_metadata.json
│               │   ├── _SUCCESS
│               │   └── part-00000-e4899cf1-e5a8-4b87-8702-fca302ab8442-c000.json
│               ├── v3
│               │   ├── _SUCCESS
│               │   ├── part-00000-1199f2af-cb31-4e9e-a6c1-747fbaf9a013-c000.json
│               │   └── process_metadata.json
│               ├── v4
│               │   ├── \ process_metadata.json
│               │   ├── _SUCCESS
│               │   └── part-00000-77ffb9dd-c7e8-44b1-88e8-411fa8c1d192-c000.json
│               └── v5
│                   ├── \ process_metadata.json
│                   ├── _SUCCESS
│                   └── part-00000-5d8e15ba-6fca-4688-a5c8-a6eacc436227-c000.json
└── reference
    └── mpns_v8
        └── annotation_labels.json

45 directories, 55 files
```


## All

```bash
tree -L 7
```

```
.
├── Dockerfile
├── LICENSE
├── README.md
├── TREE.md
├── data
│   ├── analysis
│   │   └── mpns
│   │       ├── mpns_v8
│   │       │   ├── botanical_names
│   │       │   │   └── name_mappings_v3_botanical_name_type_counts.json
│   │       │   └── name_relationships
│   │       │       ├── _SUCCESS
│   │       │       ├── part-00000-154f8e08-6d89-47be-a53f-7314d2846a45-c000.snappy.parquet
│   │       │       └── query
│   │       │           ├── _SUCCESS
│   │       │           └── part-00000-cf29ee2b-89ec-49c3-b966-d200661651e5-c000.json
│   │       └── sample_mpns_v8
│   │           └── name_relationships
│   │               ├── _SUCCESS
│   │               ├── part-00000-f24ff1dc-e046-4a26-9c0d-bbf10c28aa14-c000.json
│   │               └── query
│   │                   ├── _SUCCESS
│   │                   └── part-00000-785016e9-b0b3-482d-92cc-297783a37b7b-c000.json
│   ├── mpns
│   │   ├── mpns_v8
│   │   │   ├── medicinal_mpns_non_scientific_names.csv
│   │   │   ├── medicinal_mpns_plants.csv
│   │   │   └── medicinal_mpns_synonyms.csv
│   │   └── sample_mpns_v8
│   │       ├── medicinal_mpns_non_scientific_names.csv
│   │       ├── medicinal_mpns_plants.csv
│   │       └── medicinal_mpns_synonyms.csv
│   ├── processed
│   │   └── mpns
│   │       ├── mpns_v8
│   │       │   └── mpns_name_mappings
│   │       │       ├── v1
│   │       │       │   ├── \ process_metadata.json
│   │       │       │   ├── _SUCCESS
│   │       │       │   ├── part-00000-037e035e-947e-4d19-82e7-f5a41c5dc82a-c000.json.gz
│   │       │       │   ├── part-00001-037e035e-947e-4d19-82e7-f5a41c5dc82a-c000.json.gz
│   │       │       │   ├── part-00002-037e035e-947e-4d19-82e7-f5a41c5dc82a-c000.json.gz
│   │       │       │   ├── part-00003-037e035e-947e-4d19-82e7-f5a41c5dc82a-c000.json.gz
│   │       │       │   └── part-00004-037e035e-947e-4d19-82e7-f5a41c5dc82a-c000.json.gz
│   │       │       ├── v2
│   │       │       │   ├── \ process_metadata.json
│   │       │       │   ├── _SUCCESS
│   │       │       │   ├── scientific_name_type=plant
│   │       │       │   ├── scientific_name_type=sci_cited_medicinal
│   │       │       │   └── scientific_name_type=synonym
│   │       │       ├── v2_non_partitioned
│   │       │       │   ├── \ process_metadata.json
│   │       │       │   ├── _SUCCESS
│   │       │       │   ├── part-00000-61712b09-deaf-4154-b58b-78f44bcd2f0d-c000.snappy.parquet
│   │       │       │   ├── part-00001-61712b09-deaf-4154-b58b-78f44bcd2f0d-c000.snappy.parquet
│   │       │       │   ├── part-00002-61712b09-deaf-4154-b58b-78f44bcd2f0d-c000.snappy.parquet
│   │       │       │   ├── part-00003-61712b09-deaf-4154-b58b-78f44bcd2f0d-c000.snappy.parquet
│   │       │       │   └── part-00004-61712b09-deaf-4154-b58b-78f44bcd2f0d-c000.snappy.parquet
│   │       │       ├── v3
│   │       │       │   ├── _SUCCESS
│   │       │       │   ├── process_metadata.json
│   │       │       │   ├── scientific_name_type=__HIVE_DEFAULT_PARTITION__
│   │       │       │   ├── scientific_name_type=plant
│   │       │       │   ├── scientific_name_type=sci_cited_medicinal
│   │       │       │   └── scientific_name_type=synonym
│   │       │       ├── v4
│   │       │       │   ├── \ process_metadata.json
│   │       │       │   ├── _SUCCESS
│   │       │       │   ├── scientific_name_type=plant
│   │       │       │   ├── scientific_name_type=sci_cited_medicinal
│   │       │       │   └── scientific_name_type=synonym
│   │       │       └── v5
│   │       │           ├── \ process_metadata.json
│   │       │           ├── _SUCCESS
│   │       │           ├── scientific_name_type=plant
│   │       │           ├── scientific_name_type=sci_cited_medicinal
│   │       │           └── scientific_name_type=synonym
│   │       └── sample_mpns_v8
│   │           └── mpns_name_mappings
│   │               ├── test
│   │               │   ├── sample_intermediate_names_v3.json
│   │               │   └── sample_names_v3.json
│   │               ├── v1
│   │               │   ├── \ process_metadata.json
│   │               │   ├── _SUCCESS
│   │               │   └── part-00000-e465cfb7-f1d6-4105-bb52-dec50e24879b-c000.json
│   │               ├── v2
│   │               │   ├── \ process_metadata.json
│   │               │   ├── _SUCCESS
│   │               │   └── part-00000-e4899cf1-e5a8-4b87-8702-fca302ab8442-c000.json
│   │               ├── v3
│   │               │   ├── _SUCCESS
│   │               │   ├── part-00000-1199f2af-cb31-4e9e-a6c1-747fbaf9a013-c000.json
│   │               │   └── process_metadata.json
│   │               ├── v4
│   │               │   ├── \ process_metadata.json
│   │               │   ├── _SUCCESS
│   │               │   └── part-00000-77ffb9dd-c7e8-44b1-88e8-411fa8c1d192-c000.json
│   │               └── v5
│   │                   ├── \ process_metadata.json
│   │                   ├── _SUCCESS
│   │                   └── part-00000-5d8e15ba-6fca-4688-a5c8-a6eacc436227-c000.json
│   └── reference
│       └── mpns_v8
│           └── annotation_labels.json
├── ops_requirements.txt
├── requirements.txt
├── src
│   ├── A_mpns_v8_processing
│   │   ├── README.md
│   │   ├── input_schemas.py
│   │   ├── mpns_v8_processing_v1.py
│   │   ├── mpns_v8_processing_v2.py
│   │   ├── mpns_v8_processing_v3.py
│   │   ├── mpns_v8_processing_v4.py
│   │   ├── mpns_v8_processing_v5.py
│   │   └── output_schemas.py
│   ├── B_create_labels_for_annotation
│   │   └── README.md
│   ├── C_mpns_v8_botanical_name_analysis
│   │   ├── README.md
│   │   ├── analyse_mpns_v8_name_mappings_v3.py
│   │   └── botanical_name_regexes.py
│   ├── D_mpns_v8_name_relationships_analysis
│   │   ├── README.md
│   │   ├── analyse_mpns_v8_name_relationships.py
│   │   ├── input_schemas.py
│   │   └── output_schemas.py
│   └── E_get_mpns_v8_analyses_answers
│       ├── README.md
│       └── query_mpns_v8_name_relationships.py
├── tasks.py
└── tests
    └── helpers
        └── test_botanical_name_regexes.py

57 directories, 84 files
```