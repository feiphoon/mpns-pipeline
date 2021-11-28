import re
import pytest

from src.helpers.botanical_name_regexes import *


class TestSpeciesRegex:
    @pytest.mark.parametrize(
        "input",
        [
            ("Vicia faba L."),
            ("Vincetoxicum polyanthum Kuntze"),
            ("Vismia baccifera (L.) Planch. & Triana"),
            ("Taraxacum mongolicum Hand.-Mazz."),
            ("Fake taxon Double-barrelled Name"),
        ],
    )
    def test_species_regex__success(self, input):
        assert re.match(SPECIES_REGEX, input)

    # def test_species_regex__fail(self):
    #     """
    #     Reject species names with no author name in any form.
    #     """
    #     input = "Vicia faba"
    #     assert not re.match(SPECIES_REGEX, input)


class TestSpeciesAggregateRegex:
    """
    Testcases do not contain agg. as specified in the paper as not present in MPNS.
    """

    @pytest.mark.parametrize(
        "input",
        [
            ("Alchemilla vulgaris aggr. auct."),
            ("Taraxacum officinale aggr. F.H.Wigg."),
        ],
    )
    def test_species_aggregate_regex(self, input):
        assert re.match(SPECIES_AGGREGATE_REGEX, input)


class TestIntergenericHybridRegex:
    """Not present in MPNS."""

    @pytest.mark.parametrize(
        "input",
        [
            ("× caillei (A.Chev.) Stevels"),
            ("× hexapetala Salm-Dyck"),
            ("× bicolor Schult."),
            ("x bicolor Schult."),
            ("+ bicolor Schult."),
        ],
    )
    def test_intergeneric_hybrid_regex(self, input):
        assert re.match(INTERGENERIC_HYBRID_REGEX, input)


class TestInterspecificHybridRegex:
    @pytest.mark.parametrize(
        "input",
        [
            ("Abelmoschus × caillei (A.Chev.) Stevels"),
            ("Aloe × hexapetala Salm-Dyck"),
            ("Aconitum × bicolor Schult."),
            ("Aconitum x bicolor Schult."),
            ("Aconitum + bicolor Schult."),
        ],
    )
    def test_interspecific_hybrid_regex(self, input):
        assert re.match(INTERSPECIFIC_HYBRID_REGEX, input)

    # def test_interspecific_hybrid_regex__full_name(self):
    #     """
    #     This example could not be found in the MPNS - it's from the paper. So will be excluded.
    #     """
    #     assert re.match(
    #         INTERSPECIFIC_HYBRID_FULL_NAME_REGEX, "Primula veris × vulgaris"
    #     )


class TestSubspeciesRegex:
    @pytest.mark.parametrize(
        "input",
        [
            ("Abelmoschus moschatus subsp. biakensis (Hochr.) Borss.Waalk."),
            (
                "Abies cilicica subsp. cilicica"
            ),  # These sometimes have no author names in MPNS
        ],
    )
    def test_subspecies_regex(self, input):
        assert re.match(SUBSPECIES_REGEX, input)


class TestBotanicalVarietyRegex:
    @pytest.mark.parametrize(
        "input",
        [
            ("Viola adunca var. kirkii V.G.Durán"),
            ("Abies sachalinensis var. mayriana Miyabe & Kudô"),
            (
                "Vicia johannis Tamamsch. var. procumbens H.I.Schäf."
            ),  # From paper, not in MPNS
        ],
    )
    def test_botanical_variety_regex(self, input):
        assert re.match(BOTANICAL_VARIETY_REGEX, input)


class TestCultivarGroupRegex:
    """Not present in MPNS."""

    @pytest.mark.parametrize(
        "input",
        [
            ("Vicia faba L. Longpod Group"),
            ("Rosa Hybrid-Tea Group"),
        ],
    )
    def test_cultivar_group_regex(self, input):
        assert re.match(CULTIVAR_GROUP_REGEX, input)


class TestCultivarRegex:
    """Not present in MPNS."""

    @pytest.mark.parametrize(
        "input",
        [
            ("Vicia faba L. cv. Aquadulce"),
            ("Vicia faba L. 'Aquadulce'"),
            ('Vicia faba L. "Aquadulce"'),
            ("Pisum sativum L. (Sugar Pea Group) cv. Olympia"),
            ("Pisum sativum L. (Sugar Pea Group)'Olympia'"),
        ],
    )
    def test_cultivar_regex(self, input):
        assert re.match(CULTIVAR_REGEX, input)
