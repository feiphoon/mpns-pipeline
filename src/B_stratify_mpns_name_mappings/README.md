# B: Stratify MPNS name mappings

This stage is to pick some sets of name mappings to give a good base and balance of scientific to non-scientific name type mappings. The previous stage produced over 3 million name mappings to work with.

## The input data

The input data comes from `data/processed/mpns/mpns_v8/mpns_name_mappings/v2/`. It will be partitioned by `scientific_name_type`, so:
- `plant`
- `synonym`
- `sci_cited_medicinal`

This allows us to have a good selection of "scientific" names against common & pharmaceutical names, that we can pick from for synthetic annotation.

The following is the data schema represented in JSON format (`scientific_name_type` actually appears as partitions - so the data has to be read into Spark for them to appear this way):

```json
{
    "scientific_name_id": "wcsCmp922693",
    "scientific_name": "Bellis armena Boiss.",
    "scientific_name_type": "synonym",
    "non_scientific_name_id": "wcsCmp922692",
    "non_scientific_name": "common daisy",
    "non_scientific_name_type": "common",
    "mapping_id": 1
}
```


## Stratification

Stratification for training will be applied at the top or `scientific_name_type` level, but not at the `non_scientific_name_type` level. The rows will be shuffled within each `scientific_name_type`.

- Scientific
    - Plant
    - Synonym
    - Sci_cited_medicinal
- Non-Scientific
    - Common
    - Pharmaceutical
