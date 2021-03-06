# A: MPNS v8 Processing

This stage is to process the MPNS v8 data into a name mappings JSON file. This process is not part of the normal abstract retrieval run because this should only be done **once** after receiving a version of data.

This is specifically pegged to v8 as we make no assumptions about the schemas and categories of future versions of the MPNS - a separate version of code and schemas should be written for other data versions.

This stage was done in Spark because, in descending order of importance
- The delimiters in the original files (`|` in two and `;` in one) are intentional choices to allow for all types of possible characters in the data. Processing this data and attempting to rewrite it back to CSV caused a lot of escaping issues. Spark handles the entire process neatly.
- For the above reason, we need to write the mappings to parquet to provide a robust solution that preserves all possible characters in the data. This is because parquet format uses a safe way to delimit columns that won't cause issues with the data.
- It was straightforward to map to a different file format.
- It was straightforward to load and write against pre-defined schemas.
- In case of larger data which can't be handled on a local machine, this code can be run in Google Colab or a cloud distributed processing microservice.
- Casting and transformations were painless.

Two processing versions are available at this stage - this was because of improvements. The one in use will be v5; previous versions have been kept for posterity and will be useful to explain the evolution of this process.

## Processing V5

#### Output

##### v5 `mpns_name_mappings/*.parquet`

The following parquet format can be observed in the real data run:

| scientific_name_id | scientific_name                  | scientific_name_type | scientific_name_length | common_names                                                                                                                   | pharmaceutical_names                | common_name_count | pharmaceutical_name_count | non_scientific_name_count | mapping_id |
|--------------------|----------------------------------|----------------------|------------------------|--------------------------------------------------------------------------------------------------------------------------------|-------------------------------------|-------------------|---------------------------|---------------------------|------------|
| wcsCmp922693       | Bellis armena Boiss.             | synonym              | 20                     | null                                                                                                                           | null                                | 0                 | 0                         | 0                         | 1          |
| wcsCmp922694       | Bellis hortensis Mill.           | synonym              | 22                     | null                                                                                                                           | null                                | 0                 | 0                         | 0                         | 2          |
| wcsCmp922695       | Bellis hybrida Ten.              | synonym              | 19                     | null                                                                                                                           | null                                | 0                 | 0                         | 0                         | 3          |
| wcsCmp922692       | Bellis perennis                  | sci_cited_medicinal  | 15                     | [[bellide, wcsCmp922692, 7], [bellis perennis, wcsCmp922692, 15], [chu ju, wcsCmp922692, 6], [common daisy, wcsCmp922692, 12]] | [[made-up daisy, wcsCmp922692, 13]] | 4                 | 1                         | 5                         | 4          |
| wcsCmp922692       | Bellis perennis L.               | plant                | 18                     | [[bellide, wcsCmp922692, 7], [bellis perennis, wcsCmp922692, 15], [chu ju, wcsCmp922692, 6], [common daisy, wcsCmp922692, 12]] | [[made-up daisy, wcsCmp922692, 13]] | 4                 | 1                         | 5                         | 5          |
| wcsCmp922692       | Bellis perennis L.               | sci_cited_medicinal  | 18                     | [[bellide, wcsCmp922692, 7], [bellis perennis, wcsCmp922692, 15], [chu ju, wcsCmp922692, 6], [common daisy, wcsCmp922692, 12]] | [[made-up daisy, wcsCmp922692, 13]] | 4                 | 1                         | 5                         | 6          |
| wcsCmp922692       | Bellis perennis Linnaeus         | sci_cited_medicinal  | 24                     | [[bellide, wcsCmp922692, 7], [bellis perennis, wcsCmp922692, 15], [chu ju, wcsCmp922692, 6], [common daisy, wcsCmp922692, 12]] | [[made-up daisy, wcsCmp922692, 13]] | 4                 | 1                         | 5                         | 7          |
| wcsCmp922696       | Bellis pumila Arv.-Touv. & Dupuy | synonym              | 32                     | null                                                                                                                           | null                                | 0                 | 0                         | 0                         | 8          |


##### v5 `processing_metadata.json`

Example:

```json
{
    "total_count": 8,
    "plant_name_count": 1,
    "synonym_name_count": 4,
    "sci_cited_medicinal_name_count": 3
}
```
--------------

## Processing V4

### Transformations in a run

1. The Plants, Synonyms and Non-Scientific Names datasets are loaded against their respected schemas from `data/mpns/mpns_v8`:
    - `medicinal_mpns_plants.csv`
    - `medicinal_mpns_synonyms.csv`
    - `medicinal_mpns_non_scientific_names.csv`
1. The "Low" (`L`) quality rating and "Misapplied" (`Misapplied`) taxanomy status entries are filtered out from the Plants and Synonyms datasets.
1. The `common` & `pharmaceutical` names in the Non-Scientific Name dataset are separated from the `sci_cited_medicinal` entries.
1. Each of the main items in the Plants, Synonyms and Sci-Cited-Medicinal datasets are matched against any entries in the Common & Pharmaceutical Name datasets, which produces an exploded mapping of the former names to the names in the latter.
1. The three name mapping datasets are unioned and each row is given a **contiguous unique numerical ID** as a `mapping_id`. This is an important detail as providing a unique numerical ID in Spark is normally done with `monotonically_increasing_id`, that allows for efficient execution across distributed workers, but it does not produce a contiguous sequence, which may be confusing and later on troublesome during shuffling and splitting the mappings dataset for train/validation/test. This is instead done by the `row_number` Windowing function. However as no partition could be specified for this statement, there is a risk of running out of memory during this operation if the input data is large enough. It is important to note also that neither of these methods are deterministic, so the generation of the `mapping_id` is not idempotent.
1. The resulting `mpns_name_mappings` dataset is repartitioned to several parquet files (against an output schema) and output at `data/processed/mpns/mpns_v8/mpns_name_mappings/v2/`. The output is partitioned by `scientific_name_type`, so `plant`, `synonym` & `sci_cited_medicinal` folders are produced. It will make it easier to manage mappings for training later.
1. A `processing_metadata.json` file is also produced with counts on the mappings (see last part of this v2 section).

### Executing a test run

There are sample datasets at `data/mpns/sample_mpns_v8/v2`. These files contain a select sample of items (all linked to the same 1 plant) from the real datasets, and only exist for demonstration purposes.
- `medicinal_mpns_plants.csv`: 3 rows, only 1 of which should pass the filter
- `medicinal_mpns_synonyms.csv`: 6 rows, only 4 of which should pass the filter
- `medicinal_mpns_non_scientific_names.csv`: 7 rows, which are split into `common` & `pharmaceutical` names, and `sci_cited_medicinal` names. Only the latter are filtered. joined to matches against the total of 5 records from the first two datasets.
    - `common`: 5
    - `pharmaceutical`: 0
    - `sci_cited_medicinal`: 3

After filtering and joining, this produces the resulting `mpns_name_mappings` (at `data/processed/mpns/sample_mpns_v8/mpns_name_mappings/v2/`) of 32 rows. (No automated tests were written for this to demonstrate it).

To run this for demonstration purposes, go to `src/C_mpns_v8_processing/mpns_v8_processing_v2.py`, uncomment the relevant block of filepaths and code for the sample runs in the statements at the bottom of the file (comment out the real run). Sample data is **coalesced to 1 file**.

Then run:
```bash
inv ps.build-no-cache;inv ps.sample-mpns-v8-processing-run-v4
```

### Executing an actual run

To run this for actual reprocessing, go to `src/C_mpns_v8_processing/mpns_v8_processing_v4.py`, , uncomment the relevant block of filepaths and code for the real runs in the statements at the bottom of the file (comment out the sample run). Real data is **repartitioned to 5 files and compressed to gzip format**.

Then run:
```bash
inv ps.build-no-cache;inv ps.mpns-v8-processing-run-v4
```

### Schemas

#### Input:

##### `medicinal_mpns_plants.csv`


| name_id      | ipni_id  | taxon_status | quality_rating | rank    | family     | genus      | genus_hybrid | species  | species_hybrid | infra_species | parent_author | primary_author | full_scientific_name     |
|--------------|----------|--------------|----------------|---------|------------|------------|--------------|----------|----------------|---------------|---------------|----------------|--------------------------|
| wcsCmp922692 | 184409-1 | Accepted     | H              | species | Asteraceae | Bellis     | null         | perennis | null           | null          | null          | L.             | Bellis perennis L.       |
| wcsCmp672852 | 703219-1 | Accepted     | L              | species | Proteaceae | Bellendena | null         | montana  | null           | null          | null          | R.Br.          | Bellendena montana R.Br. |
| wcsCmp672852 | 703219-1 | Misapplied   | H              | species | Proteaceae | Bellendena | null         | montana  | null           | null          | null          | R.Br.          | Bellendena montana R.Br. |



##### `medicinal_mpns_synonyms.csv`


| name_id      | ipni_id  | taxon_status | quality_rating | rank    | genus  | genus_hybrid | species   | species_hybrid | infra_species | parent_author | primary_author     | full_scientific_name             | acc_name_id  |
|--------------|----------|--------------|----------------|---------|--------|--------------|-----------|----------------|---------------|---------------|--------------------|----------------------------------|--------------|
| wcsCmp922693 | 184363-1 | Synonym      | M              | species | Bellis | null         | armena    | null           | null          | null          | Boiss.             | Bellis armena Boiss.             | wcsCmp922692 |
| wcsCmp922694 | 184383-1 | Synonym      | M              | species | Bellis | null         | hortensis | null           | null          | null          | Mill.              | Bellis hortensis Mill.           | wcsCmp922692 |
| wcsCmp922695 | 184384-1 | Synonym      | M              | species | Bellis | null         | hybrida   | null           | null          | null          | Ten.               | Bellis hybrida Ten.              | wcsCmp922692 |
| wcsCmp922696 | 184413-1 | Synonym      | M              | species | Bellis | null         | pumila    | null           | null          | null          | Arv.-Touv. & Dupuy | Bellis pumila Arv.-Touv. & Dupuy | wcsCmp922692 |
| wcsCmp922693 | 184363-1 | Synonym      | L              | species | Bellis | null         | armena    | null           | null          | null          | Boiss.             | Bellis armena Boiss.             | wcsCmp922692 |
| wcsCmp922693 | 184363-1 | Misapplied   | M              | species | Bellis | null         | armena    | null           | null          | null          | Boiss.             | Bellis armena Boiss.             | wcsCmp922692 |


##### `medicinal_mpns_non_scientific_names.csv`

| name_type           | name                     | plant_id     | name_id      |
|---------------------|--------------------------|--------------|--------------|
| common              | bellide                  | wcsCmp922692 | wcsCmp922692 |
| common              | belliric myrobalan       | wcsCmp431540 | wcsCmp431540 |
| common              | bellis perennis          | wcsCmp922692 | wcsCmp922692 |
| sci_cited_medicinal | Bellis perennis          | wcsCmp922692 | wcsCmp922692 |
| sci_cited_medicinal | Bellis perennis L.       | wcsCmp922692 | wcsCmp922692 |
| sci_cited_medicinal | Bellis perennis Linnaeus | wcsCmp922692 | wcsCmp922692 |
| common              | chu ju                   | wcsCmp922692 | wcsCmp922692 |
| common              | common daisy             | wcsCmp922692 | wcsCmp922692 |


#### Output

##### v4 `mpns_name_mappings/*.parquet`

The following parquet format can be observed in the real data run:

| scientific_name_id | scientific_name       | non_scientific_name_id | non_scientific_name | non_scientific_name_type | scientific_name_type | mapping_id | scientific_name_length | non_scientific_name_length |
|--------------------|-----------------------|------------------------|---------------------|--------------------------|----------------------|------------|------------------------|----------------------------|
| wcsCmp922693       | Bellis armena Boiss.  | wcsCmp922692           | common daisy        | common                   | synonym              | 1          | 20                     | 12                         |
| wcsCmp922693       | Bellis armena Boiss.  | wcsCmp922692           | chu ju              | common                   | synonym              | 2          | 20                     | 6                          |
| wcsCmp922693       | Bellis armena Boiss.  | wcsCmp922692           | bellis perennis     | common                   | synonym              | 3          | 20                     | 15                         |
| wcsCmp922693       | Bellis armena Boiss.  | wcsCmp922692           | bellide             | common                   | synonym              | 4          | 20                     | 7                          |
| wcsCmp922692       | Bellis perennis       | wcsCmp922692           | common daisy        | common                   | sci_cited_medicinal  | 13         | 15                     | 12                         |
| wcsCmp922692       | Bellis perennis       | wcsCmp922692           | chu ju              | common                   | sci_cited_medicinal  | 14         | 15                     | 6                          |
| wcsCmp922692       | Bellis perennis       | wcsCmp922692           | bellis perennis     | common                   | sci_cited_medicinal  | 15         | 15                     | 15                         |
| wcsCmp922692       | Bellis perennis       | wcsCmp922692           | bellide             | common                   | sci_cited_medicinal  | 16         | 15                     | 7                          |
| wcsCmp922692       | Bellis perennis L.    | wcsCmp922692           | common daisy        | common                   | plant                | 17         | 18                     | 12                         |
| wcsCmp922692       | Bellis perennis L.    | wcsCmp922692           | chu ju              | common                   | plant                | 18         | 18                     | 6                          |
| wcsCmp922692       | Bellis perennis L.    | wcsCmp922692           | bellis perennis     | common                   | plant                | 19         | 18                     | 15                         |
| wcsCmp922692       | Bellis perennis L.    | wcsCmp922692           | bellide             | common                   | plant                | 20         | 18                     | 7                          |


JSON representation for easier reading (can be observed in the sample run):

```json
{
    "scientific_name_id": "wcsCmp922693", // This was from the synonym name_id
    "scientific_name": "Bellis armena Boiss.",
    "scientific_name_type": "synonym",
    "scientific_name_length": 20,
    "non_scientific_name_id": "wcsCmp922692", // This was from the non_scientific_name name_id
    "non_scientific_name": "common daisy",
    "non_scientific_name_type": "common",
    "non_scientific_name_length": 12,
    "mapping_id": 1
}
```


##### v4 `processing_metadata.json`

Example:

```json
{
    "total_count": 35,
    "synonym_name_count": 28,
    "plant_name_count": 7,
    "common_name_count": 20,
    "pharmaceutical_name_count": 0,
    "sci_cited_medicinal_name_count": 15
}
```

-----------

## Processing V3

No documentation - the only difference between this version and the previous V2 version, is that the
non_scientific_names are grouped under each scientific name. This version is no longer used as despite being richer,
the unexploded format made it harder to work with in the NER pipeline.

-----------

## Processing V2

### Transformations in a run

1. The Plants, Synonyms and Non-Scientific Names datasets are loaded against their respected schemas from `data/mpns/mpns_v8`:
    - `medicinal_mpns_plants.csv`
    - `medicinal_mpns_synonyms.csv`
    - `medicinal_mpns_non_scientific_names.csv`
1. The "Low" (`L`) quality rating and "Misapplied" (`Misapplied`) taxanomy status entries are filtered out from the Plants and Synonyms datasets.
1. The `common` & `pharmaceutical` names in the Non-Scientific Name dataset are separated from the `sci_cited_medicinal` entries.
1. Each of the main items in the Plants, Synonyms and Sci-Cited-Medicinal datasets are matched against any entries in the Common & Pharmaceutical Name datasets, which produces an exploded mapping of the former names to the names in the latter.
1. The three name mapping datasets are unioned and each row is given a **contiguous unique numerical ID** as a `mapping_id`. This is an important detail as providing a unique numerical ID in Spark is normally done with `monotonically_increasing_id`, that allows for efficient execution across distributed workers, but it does not produce a contiguous sequence, which may be confusing and later on troublesome during shuffling and splitting the mappings dataset for train/validation/test. This is instead done by the `row_number` Windowing function. However as no partition could be specified for this statement, there is a risk of running out of memory during this operation if the input data is large enough. It is important to note also that neither of these methods are deterministic, so the generation of the `mapping_id` is not idempotent.
1. The resulting `mpns_name_mappings` dataset is repartitioned to several parquet files (against an output schema) and output at `data/processed/mpns/mpns_v8/mpns_name_mappings/v2/`. The output is partitioned by `scientific_name_type`, so `plant`, `synonym` & `sci_cited_medicinal` folders are produced. It will make it easier to manage mappings for training later.
1. A `processing_metadata.json` file is also produced with counts on the mappings (see last part of this v2 section).

### Executing a test run

There are sample datasets at `data/mpns/sample_mpns_v8/v2`. These files contain a select sample of items (all linked to the same 1 plant) from the real datasets, and only exist for demonstration purposes.
- `medicinal_mpns_plants.csv`: 3 rows, only 1 of which should pass the filter
- `medicinal_mpns_synonyms.csv`: 6 rows, only 4 of which should pass the filter
- `medicinal_mpns_non_scientific_names.csv`: 7 rows, which are split into `common` & `pharmaceutical` names, and `sci_cited_medicinal` names. Only the latter are filtered. joined to matches against the total of 5 records from the first two datasets.
    - `common`: 5
    - `pharmaceutical`: 0
    - `sci_cited_medicinal`: 3

After filtering and joining, this produces the resulting `mpns_name_mappings` (at `data/processed/mpns/sample_mpns_v8/mpns_name_mappings/v2/`) of 32 rows. (No automated tests were written for this to demonstrate it).

To run this for demonstration purposes, go to `src/C_mpns_v8_processing/mpns_v8_processing_v2.py`, uncomment the relevant block of filepaths and code for the sample runs in the statements at the bottom of the file (comment out the real run). Sample data is **coalesced to 1 file**.

Then run:
```bash
inv ps.build-no-cache;inv ps.sample-mpns-v8-processing-run-v2
```

### Executing an actual run

To run this for actual reprocessing, go to `src/C_mpns_v8_processing/mpns_v8_processing_v2.py`, , uncomment the relevant block of filepaths and code for the real runs in the statements at the bottom of the file (comment out the sample run). Real data is **repartitioned to 5 files and compressed to gzip format**.

Then run:
```bash
inv ps.build-no-cache;inv ps.mpns-v8-processing-run-v2
```

### Schemas

#### Input:

##### `medicinal_mpns_plants.csv`


| name_id      | ipni_id  | taxon_status | quality_rating | rank    | family     | genus      | genus_hybrid | species  | species_hybrid | infra_species | parent_author | primary_author | full_scientific_name     |
|--------------|----------|--------------|----------------|---------|------------|------------|--------------|----------|----------------|---------------|---------------|----------------|--------------------------|
| wcsCmp922692 | 184409-1 | Accepted     | H              | species | Asteraceae | Bellis     | null         | perennis | null           | null          | null          | L.             | Bellis perennis L.       |
| wcsCmp672852 | 703219-1 | Accepted     | L              | species | Proteaceae | Bellendena | null         | montana  | null           | null          | null          | R.Br.          | Bellendena montana R.Br. |
| wcsCmp672852 | 703219-1 | Misapplied   | H              | species | Proteaceae | Bellendena | null         | montana  | null           | null          | null          | R.Br.          | Bellendena montana R.Br. |



##### `medicinal_mpns_synonyms.csv`


| name_id      | ipni_id  | taxon_status | quality_rating | rank    | genus  | genus_hybrid | species   | species_hybrid | infra_species | parent_author | primary_author     | full_scientific_name             | acc_name_id  |
|--------------|----------|--------------|----------------|---------|--------|--------------|-----------|----------------|---------------|---------------|--------------------|----------------------------------|--------------|
| wcsCmp922693 | 184363-1 | Synonym      | M              | species | Bellis | null         | armena    | null           | null          | null          | Boiss.             | Bellis armena Boiss.             | wcsCmp922692 |
| wcsCmp922694 | 184383-1 | Synonym      | M              | species | Bellis | null         | hortensis | null           | null          | null          | Mill.              | Bellis hortensis Mill.           | wcsCmp922692 |
| wcsCmp922695 | 184384-1 | Synonym      | M              | species | Bellis | null         | hybrida   | null           | null          | null          | Ten.               | Bellis hybrida Ten.              | wcsCmp922692 |
| wcsCmp922696 | 184413-1 | Synonym      | M              | species | Bellis | null         | pumila    | null           | null          | null          | Arv.-Touv. & Dupuy | Bellis pumila Arv.-Touv. & Dupuy | wcsCmp922692 |
| wcsCmp922693 | 184363-1 | Synonym      | L              | species | Bellis | null         | armena    | null           | null          | null          | Boiss.             | Bellis armena Boiss.             | wcsCmp922692 |
| wcsCmp922693 | 184363-1 | Misapplied   | M              | species | Bellis | null         | armena    | null           | null          | null          | Boiss.             | Bellis armena Boiss.             | wcsCmp922692 |


##### `medicinal_mpns_non_scientific_names.csv`

| name_type           | name                     | plant_id     | name_id      |
|---------------------|--------------------------|--------------|--------------|
| common              | bellide                  | wcsCmp922692 | wcsCmp922692 |
| common              | belliric myrobalan       | wcsCmp431540 | wcsCmp431540 |
| common              | bellis perennis          | wcsCmp922692 | wcsCmp922692 |
| sci_cited_medicinal | Bellis perennis          | wcsCmp922692 | wcsCmp922692 |
| sci_cited_medicinal | Bellis perennis L.       | wcsCmp922692 | wcsCmp922692 |
| sci_cited_medicinal | Bellis perennis Linnaeus | wcsCmp922692 | wcsCmp922692 |
| common              | chu ju                   | wcsCmp922692 | wcsCmp922692 |
| common              | common daisy             | wcsCmp922692 | wcsCmp922692 |


#### Output

##### v2 `mpns_name_mappings/*.parquet`

The following parquet format can be observed in the real data run:

| scientific_name_id | scientific_name       | non_scientific_name_id | non_scientific_name | non_scientific_name_type | scientific_name_type | mapping_id |
|--------------------|-----------------------|------------------------|---------------------|--------------------------|----------------------|------------|
| wcsCmp922693       | Bellis armena Boiss.  | wcsCmp922692           | common daisy        | common                   | synonym              | 1          |
| wcsCmp922693       | Bellis armena Boiss.  | wcsCmp922692           | chu ju              | common                   | synonym              | 2          |
| wcsCmp922693       | Bellis armena Boiss.  | wcsCmp922692           | bellis perennis     | common                   | synonym              | 3          |
| wcsCmp922693       | Bellis armena Boiss.  | wcsCmp922692           | bellide             | common                   | synonym              | 4          |
| wcsCmp922692       | Bellis perennis       | wcsCmp922692           | common daisy        | common                   | sci_cited_medicinal  | 13         |
| wcsCmp922692       | Bellis perennis       | wcsCmp922692           | chu ju              | common                   | sci_cited_medicinal  | 14         |
| wcsCmp922692       | Bellis perennis       | wcsCmp922692           | bellis perennis     | common                   | sci_cited_medicinal  | 15         |
| wcsCmp922692       | Bellis perennis       | wcsCmp922692           | bellide             | common                   | sci_cited_medicinal  | 16         |
| wcsCmp922692       | Bellis perennis L.    | wcsCmp922692           | common daisy        | common                   | plant                | 17         |
| wcsCmp922692       | Bellis perennis L.    | wcsCmp922692           | chu ju              | common                   | plant                | 18         |
| wcsCmp922692       | Bellis perennis L.    | wcsCmp922692           | bellis perennis     | common                   | plant                | 19         |
| wcsCmp922692       | Bellis perennis L.    | wcsCmp922692           | bellide             | common                   | plant                | 20         |


JSON representation for easier reading (can be observed in the sample run):

```json
{
    "scientific_name_id": "wcsCmp922693", // This was from the synonym name_id
    "scientific_name": "Bellis armena Boiss.",
    "scientific_name_type": "synonym",
    "non_scientific_name_id": "wcsCmp922692", // This was from the non_scientific_name name_id
    "non_scientific_name": "common daisy",
    "non_scientific_name_type": "common",
    "mapping_id": 1
}
```


##### v2 `processing_metadata.json`

```json
{
    "total_count": 35,
    "synonym_name_count": 28,
    "plant_name_count": 7,
    "common_name_count": 20,
    "pharmaceutical_name_count": 0,
    "sci_cited_medicinal_name_count": 15
}
```

-----------

## Processing V1

### Transformations in a run

1. The Plants, Synonyms and Non-Scientific Names datasets are loaded against their respected schemas from `data/mpns/mpns_v8`:
    - `medicinal_mpns_plants.csv`
    - `medicinal_mpns_synonyms.csv`
    - `medicinal_mpns_non_scientific_names.csv`
1. The "Low" (`L`) quality rating and "Misapplied" (`Misapplied`) taxanomy status entries are filtered out from the Plants and Synonyms datasets.
1. Each of the main items in the Plants and the Synonyms datasets are matched against any entries in the Non-Scientific Name dataset, which produces an exploded mapping of the former names to the names in the latter.
1. The two name mapping datasets are unioned and each row is given a **contiguous unique numerical ID** as a `mapping_id`. This is an important detail as providing a unique numerical ID in Spark is normally done with `monotonically_increasing_id`, that allows for efficient execution across distributed workers, but it does not produce a contiguous sequence, which may be confusing and later on troublesome during shuffling and splitting the mappings dataset for train/validation/test. This is instead done by the `row_number` Windowing function. However as no partition could be specified for this statement, there is a risk of running out of memory during this operation if the input data is large enough. It is important to note also that neither of these methods are deterministic, so the generation of the `mapping_id` is not idempotent.
1. The resulting `mpns_name_mappings` dataset is repartitioned to several single JSONLines file (against an output schema) and output at `data/processed/mpns/mpns_v8/mpns_name_mappings/v1/`
1. A `processing_metadata.json` file is also produced with counts on the mappings (see last part of this v2 section).

### Executing a test run

There are sample datasets at `data/mpns/sample_mpns_v8`. These files contain a select sample of items (all linked to the same 1 plant) from the real datasets, and only exist for demonstration purposes.
- `medicinal_mpns_plants.csv`: 3 rows, only 1 of which should pass the filter
- `medicinal_mpns_synonyms.csv`: 6 rows, only 4 of which should pass the filter
- `medicinal_mpns_non_scientific_names.csv`: 7 rows, which do not have to be filtered but joined to matches against the total of 5 records from the first two datasets.

This means we expect the resulting `mpns_name_mappings` (at `data/processed/mpns/sample_mpns_v8/mpns_name_mappings/v1/`) to contain (7 x 5) 35 rows.

To run this for demonstration purposes, go to `src/C_mpns_v8_processing/mpns_v8_processing_v1.py`, uncomment the relevant block of filepaths and code for the sample runs in the statements at the bottom of the file (comment out the real run). Sample data is **coalesced to 1 file**.

Then run:
```bash
inv ps.build-no-cache;inv ps.sample-mpns-v8-processing-run-v1
```

### Executing an actual run

To run this for actual reprocessing, go to `src/C_mpns_v8_processing/mpns_v8_processing_v1.py`, , uncomment the relevant block of filepaths and code for the real runs in the statements at the bottom of the file (comment out the sample run). Real data is **repartitioned to 5 files and compressed to gzip format**.

Then run:
```bash
inv ps.build-no-cache;inv ps.mpns-v8-processing-run-v1
```

### Schemas

#### Input:

##### `medicinal_mpns_plants.csv`


| name_id      | ipni_id  | taxon_status | quality_rating | rank    | family     | genus      | genus_hybrid | species  | species_hybrid | infra_species | parent_author | primary_author | full_scientific_name     |
|--------------|----------|--------------|----------------|---------|------------|------------|--------------|----------|----------------|---------------|---------------|----------------|--------------------------|
| wcsCmp922692 | 184409-1 | Accepted     | H              | species | Asteraceae | Bellis     | null         | perennis | null           | null          | null          | L.             | Bellis perennis L.       |
| wcsCmp672852 | 703219-1 | Accepted     | L              | species | Proteaceae | Bellendena | null         | montana  | null           | null          | null          | R.Br.          | Bellendena montana R.Br. |
| wcsCmp672852 | 703219-1 | Misapplied   | H              | species | Proteaceae | Bellendena | null         | montana  | null           | null          | null          | R.Br.          | Bellendena montana R.Br. |



##### `medicinal_mpns_synonyms.csv`


| name_id      | ipni_id  | taxon_status | quality_rating | rank    | genus  | genus_hybrid | species   | species_hybrid | infra_species | parent_author | primary_author     | full_scientific_name             | acc_name_id  |
|--------------|----------|--------------|----------------|---------|--------|--------------|-----------|----------------|---------------|---------------|--------------------|----------------------------------|--------------|
| wcsCmp922693 | 184363-1 | Synonym      | M              | species | Bellis | null         | armena    | null           | null          | null          | Boiss.             | Bellis armena Boiss.             | wcsCmp922692 |
| wcsCmp922694 | 184383-1 | Synonym      | M              | species | Bellis | null         | hortensis | null           | null          | null          | Mill.              | Bellis hortensis Mill.           | wcsCmp922692 |
| wcsCmp922695 | 184384-1 | Synonym      | M              | species | Bellis | null         | hybrida   | null           | null          | null          | Ten.               | Bellis hybrida Ten.              | wcsCmp922692 |
| wcsCmp922696 | 184413-1 | Synonym      | M              | species | Bellis | null         | pumila    | null           | null          | null          | Arv.-Touv. & Dupuy | Bellis pumila Arv.-Touv. & Dupuy | wcsCmp922692 |
| wcsCmp922693 | 184363-1 | Synonym      | L              | species | Bellis | null         | armena    | null           | null          | null          | Boiss.             | Bellis armena Boiss.             | wcsCmp922692 |
| wcsCmp922693 | 184363-1 | Misapplied   | M              | species | Bellis | null         | armena    | null           | null          | null          | Boiss.             | Bellis armena Boiss.             | wcsCmp922692 |


##### `medicinal_mpns_non_scientific_names.csv`

| name_type           | name                     | plant_id     | name_id      |
|---------------------|--------------------------|--------------|--------------|
| common              | bellide                  | wcsCmp922692 | wcsCmp922692 |
| common              | belliric myrobalan       | wcsCmp431540 | wcsCmp431540 |
| common              | bellis perennis          | wcsCmp922692 | wcsCmp922692 |
| sci_cited_medicinal | Bellis perennis          | wcsCmp922692 | wcsCmp922692 |
| sci_cited_medicinal | Bellis perennis L.       | wcsCmp922692 | wcsCmp922692 |
| sci_cited_medicinal | Bellis perennis Linnaeus | wcsCmp922692 | wcsCmp922692 |
| common              | chu ju                   | wcsCmp922692 | wcsCmp922692 |
| common              | common daisy             | wcsCmp922692 | wcsCmp922692 |


#### Output

##### v1 `mpns_name_mappings.json`


| full_scientific_name_id | full_scientific_name   | non_scientific_name      | non_scientific_name_type | is_synonym | mapping_id |
|-------------------------|------------------------|--------------------------|--------------------------|------------|------------|
| wcsCmp922693            | Bellis armena Boiss.   | common daisy             | common                   | true       | 1          |
| wcsCmp922693            | Bellis armena Boiss.   | chu ju                   | common                   | true       | 2          |
| wcsCmp922693            | Bellis armena Boiss.   | Bellis perennis Linnaeus | sci_cited_medicinal      | true       | 3          |
| wcsCmp922693            | Bellis armena Boiss.   | Bellis perennis L.       | sci_cited_medicinal      | true       | 4          |
| wcsCmp922693            | Bellis armena Boiss.   | Bellis perennis          | sci_cited_medicinal      | true       | 5          |
| wcsCmp922693            | Bellis armena Boiss.   | bellis perennis          | common                   | true       | 6          |
| wcsCmp922693            | Bellis armena Boiss.   | bellide                  | common                   | true       | 7          |
| wcsCmp922694            | Bellis hortensis Mill. | common daisy             | common                   | true       | 8          |
| wcsCmp922694            | Bellis hortensis Mill. | chu ju                   | common                   | true       | 9          |
| wcsCmp922694            | Bellis hortensis Mill. | Bellis perennis Linnaeus | sci_cited_medicinal      | true       | 10         |
| wcsCmp922694            | Bellis hortensis Mill. | Bellis perennis L.       | sci_cited_medicinal      | true       | 11         |
| wcsCmp922694            | Bellis hortensis Mill. | Bellis perennis          | sci_cited_medicinal      | true       | 12         |
| wcsCmp922694            | Bellis hortensis Mill. | bellis perennis          | common                   | true       | 13         |
| wcsCmp922694            | Bellis hortensis Mill. | bellide                  | common                   | true       | 14         |
| wcsCmp922695            | Bellis hybrida Ten.    | common daisy             | common                   | true       | 15         |
| wcsCmp922695            | Bellis hybrida Ten.    | chu ju                   | common                   | true       | 16         |
| wcsCmp922695            | Bellis hybrida Ten.    | Bellis perennis Linnaeus | sci_cited_medicinal      | true       | 17         |
| wcsCmp922695            | Bellis hybrida Ten.    | Bellis perennis L.       | sci_cited_medicinal      | true       | 18         |
| wcsCmp922695            | Bellis hybrida Ten.    | Bellis perennis          | sci_cited_medicinal      | true       | 19         |
| wcsCmp922695            | Bellis hybrida Ten.    | bellis perennis          | common                   | true       | 20         |
only showing top 20 rows


JSONL version:

```json
{
    "full_scientific_name_id": "wcsCmp922693",
    "full_scientific_name": "Bellis armena Boiss.",
    "non_scientific_name": "common daisy",
    "non_scientific_name_type": "common",
    "is_synonym": true,
    "mapping_id": 1
}
```

##### v1 `processing_metadata.json`

```json
{
    "total_count": 35,
    "is_synonym_count": 28,
    "is_not_synonym_count": 7,
    "is_common_name_count": 20,
    "is_pharmaceutical_name_count": 0,
    "is_sci_cited_medicinal_name_count": 15
}
```
