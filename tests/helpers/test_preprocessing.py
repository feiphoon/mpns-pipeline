import pytest
from src.helpers.preprocessing import snakify_text


class TestSnakifyText:
    @pytest.mark.parametrize(
        "input, expected",
        [
            ("bana", "bana"),
            ("ba na na", "ba_na_na"),
            ("BANA", "bana"),
            ("ba-na", "ba_na"),
            (" ba", "ba"),
            ("ba ", "ba"),
            ("ba  na", "ba_na"),
            ("ba   na", "ba_na"),
            ("ba.", "ba"),
            ("(ba)", "ba"),
            ("'ba", "ba"),
            ("ba/", "ba"),
            ("ba\\", "ba"),
            ('"ba"', "ba"),
            ("`ba", "ba"),
            ("ba & na", "ba_na"),
            ("‘ba’", "ba"),
            ("“ba”", "ba"),
            ("baña", "bana"),
            ("bá", "ba"),
            ("ba#", "ba"),
            ("ba, na", "ba_na"),
            ("ba,na", "ba_na"),
        ],
    )
    def test_snakify_text(self, input, expected):
        result = snakify_text(input)
        assert result == expected

    @pytest.mark.parametrize(
        "input, expected",
        [
            ("n\\goalla", "ngoalla"),
            ("\\gu:'i", "gui"),
            ("3aqir qir’he", "3aqir_qirhe"),
            ("3ên el cerrad", "3en_el_cerrad"),
            ("3ên el-dîk", "3en_el_dik"),
            ("3eşruq", "3esruq"),
            ("zίζυφον zizyfon", "z_zizyfon"),
            ("계수나무가지", ""),
            ("เก๋ากี่", ""),
            ("สมอ ไทย", "_"),
            ("ミ ロバランノキ", "_"),
            ("ちんぴ", ""),
            ("ブドウ酒", ""),
            ("ابرة الراهب", "_"),
            ("ясен, лист", "_"),
            ("आम", ""),
            ("हरा", ""),
            ("一枝黄花", ""),
        ],
    )
    def test_snakify_text__real_non_scientific_names(self, input, expected):
        result = snakify_text(input)
        assert result == expected

    @pytest.mark.parametrize(
        "input, expected",
        [
            (
                'Anisopappus chinensis subsp. buchwaldii (O.Hoffm.) "S.Ortiz, Paiva & Rodr.Oubiña"',
                "anisopappus_chinensis_subsp_buchwaldii_o_hoffm_s_ortiz_paiva_rodr_oubina",
            ),
            (
                "Elephantopus scaber var. martii (Sch. Bip. / DC.) Hassk.",
                "elephantopus_scaber_var_martii_sch_bip_dc_hassk",
            ),
        ],
    )
    def test_snakify_text__real_scientific_names(self, input, expected):
        result = snakify_text(input)
        assert result == expected
