# C: MPNS v8 Botanical Name Analysis

This section is to analyse the statistics of formats of the scientific names, in the name mappings I've produced.

The botanical name types are as specified in the 1994 paper Plants in Botanical Databases.
The regexes are created by me according to the rules in the above paper - the statements are in:
[src/C_mpns_v8_botanical_name_analysis/botanical_name_regexes.py](src/C_mpns_v8_botanical_name_analysis/botanical_name_regexes.py).

Each regex pattern has test examples found from the MPNS (instead of using the ones in the paper). The tests are at:
[tests/helpers/test_botanical_name_regexes.py](tests/helpers/test_botanical_name_regexes.py). These tests can be run
using `inv test` on the CLI.

These statistics are just for discussion. Some caveats:
- The species regex is the most flexible and definitely overlaps with the other name types.
- The MPNS names which actually should matter against these standards are in the Plants and Synonyms files. Of course
there is a third MPNS name type that's deemed to be "scientific" for the purposes of this project, but is actually from
Non-Scientific Names in the category `sci_cited_medicinal`. Some of these have produced counts in botanical name type
stats such as the cultivar group name.
- As a result of possible overlaps, the sum of these counts will exceed the `total_name_mappings_count`.

The exercise of having to create these regexes for plant names has also illustrated how difficult it is to be able to
establish programmatic patterns on names which are already cleaned to a high standard (Plants and Synonyms by the MPNS
maintainers) - to try and do rule-based matching on names in the wild with data hygiene issues will be extremely
challenging and not scalable.

Format of the analysis file produced:
[name_mappings_v3_botanical_name_type_counts.json](data/analysis/mpns/mpns_v8/name_mappings_v3_botanical_name_type_counts.json)

Results:

```json
{
    "species_name_type_count": 325432,
    "species_aggregate_name_type_count": 2,
    "intergeneric_hybrid_name_type_count": 0,
    "interspecific_hybrid_name_type_count": 2203,
    "subspecies_name_type_count": 13549,
    "botanical_variety_name_type_count": 65273,
    "cultivar_group_name_type_count": 1,
    "cultivar_name_type_count": 188197,
    "total_name_mappings_count": 336167
}
```