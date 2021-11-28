SPECIES_REGEX = r"^([A-Z]{1}[a-z]+\s[a-z\(\)&.]+\s[A-zÀ-ÿ.&\(\)\w\s]+)"
SPECIES_AGGREGATE_REGEX = (
    r"^([A-Z]{1}[a-z]+\s[a-z\(\)&.]+\s(aggr.)\s[A-zÀ-ÿ.&\(\)\w\s]+)"
)
INTERGENERIC_HYBRID_REGEX = r"^([×|x|+]\s[a-z]+\s[-A-zÀ-ÿ.&\(\)\w\s]+)"
INTERSPECIFIC_HYBRID_REGEX = r"^([A-Z]{1}[a-z]+\s[×|x|+]\s[a-z]+\s[-A-zÀ-ÿ.&\(\)\w\s]+)"
# INTERSPECIFIC_HYBRID_FULL_NAME_REGEX = (
#     r"([A-Z]{1}[a-z]+\s[\w\s]+?[×|x|+]\s[a-z]+\s?[-A-zÀ-ÿ.&\(\)\w\s]+?)"
# )
SUBSPECIES_REGEX = (
    r"^([A-Z]{1}[a-z]+\s[a-z\(\)&.]+\s(subsp.)\s[a-zá-ÿ]+\s?[A-zÀ-ÿ.&\(\)\w\s]+)"
)
BOTANICAL_VARIETY_REGEX = r"^([A-Z]{1}[a-z]+\s[a-z\(\)&.]+\s?[A-zÀ-ÿ.&\(\)\w\s]+\s(var.)\s[a-zá-ÿ]+\s?[A-zÀ-ÿ.&\(\)\w\s]+)"  # noqa: E501
CULTIVAR_GROUP_REGEX = r"^([A-Z]{1}[a-z]+\s?[a-z\(\)&.]+?\s?[A-zÀ-ÿ.&\(\)\w\s]+?\s[A-Z]{1}[a-zá-ÿ.&\-\(\)\w\s]+(Group))$"  # noqa: E501
CULTIVAR_REGEX = (
    r"^([A-Z]{1}[a-z]+\s[a-z\(\)&.]+\s[A-zÀ-ÿ.&\(\)\w\s]+\s[a-zá-ÿ.&\-\(\)\w\s'\"]+)"
)
