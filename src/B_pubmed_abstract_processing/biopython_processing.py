"""
Using the biopython.Bio library - example script here:
https://github.com/biopython/biopython/blob/master/Scripts/query_pubmed.py

More:
https://towardsdatascience.com/write-a-document-classifier-in-less-than-30-minutes-2d96a8a8820c
"""
import json
import os
from datetime import date
from pathlib import Path
from typing import List

from src.helpers.navigation import get_directories, get_files
from src.helpers.datetime import convert_possible_date_to_str


# The following declarations aren't used because I had trouble unpacking namedtuples
# and dataclasses to JSON with the added complication of trying to serialise dates to string.

# class PubmedAbstractObject(NamedTuple):
#     pmid: str = None
#     doi: str = None
#     language: List[str] = None
#     title: str = None
#     abstract: str = None
#     keywords: List[str] = None
#     article_date: date = None
#     date_completed: date = None
#     date_revised: date = None


# PubmedAbstractObject = NamedTuple(
#     "PubmedAbstractObject",
#     [
#         ("pmid", str),
#         ("doi", str),
#         ("language", List[str]),
#         ("title", str),
#         ("abstract", str),
#         ("keywords", List[str]),
#         ("article_date", date),
#         ("date_completed", date),
#         ("date_revised", date),
#     ],
# )


# PubmedAbstractObject = NamedTuple(
#     "PubmedAbstractObject",
#     pmid=str,
#     doi=str,
#     language=List[str],
#     title=str,
#     abstract=str,
#     keywords=List[str],
#     article_date=date,
#     date_completed=date,
#     date_revised=date,
# )


# process_pubmed_abstracts(
#     run_input_filepath=run.create_run_filepath(pubmed_abstracts_raw_filepath),
#     run_output_filepath=run.create_run_filepath(
#         pubmed_abstracts_processed_filepath
#     ),
#     query_terms=t,
#     abstract_size_tolerance=100,
# )


def process_pubmed_abstracts(
    run_input_filepath: Path,
    run_output_filepath: Path,
    abstract_size_tolerance: int,
) -> None:
    input_folder_paths: List = get_directories(run_input_filepath)
    # input_filepath_folder_names: List = [Path(_).name for _ in _input_filepaths]

    # Per query folder, e.g. data/raw/pubmed_abstracts/2021-10-22-15-42-10/chia/
    for _input_folder in input_folder_paths:
        # Get filepath for corresponding input query metadata file.
        # To grab the original query term.

        _input_metadata_filepath: Path = Path(f"{_input_folder}/query_metadata.json")

        with _input_metadata_filepath.open("r", encoding="utf-8") as f:
            _loaded_query_metadata: dict = json.load(f)
            _query_metadata__query_term = _loaded_query_metadata["query_terms"]
            _query_metadata__results_requested = _loaded_query_metadata[
                "num_results_requested"
            ]
            _query_metadata__queries_retrieved = _loaded_query_metadata[
                "num_queries_retrieved"
            ]

        # Prepare corresponding output filepath for this input filepath
        run_output_filepath_per_query_term: Path = Path(
            f"{run_output_filepath}/{_input_folder.name}"
        )
        run_output_filepath_per_query_term.mkdir(parents=True, exist_ok=True)

        # Prepare output filepath for a metadata file per input query folder
        _processing_metadata_filepath: Path = Path(
            f"{run_output_filepath_per_query_term}/processing_metadata.json"
        )

        with open(_processing_metadata_filepath, "w+", encoding="utf-8") as f:
            _loaded_processing_metadata: dict = {}
            _loaded_processing_metadata["query_terms"] = _query_metadata__query_term
            _loaded_processing_metadata[
                "num_results_requested"
            ] = _query_metadata__results_requested
            _loaded_processing_metadata[
                "num_queries_retrieved"
            ] = _query_metadata__queries_retrieved

            json.dump(_loaded_processing_metadata, f)

        _success_count: int = 0
        _reject_count: int = 0

        # Get all available file names in the input folder,
        # Exclude the metadata file.
        input_filepaths = [
            file for file in get_files(_input_folder) if "metadata" not in file.name
        ]

        # Open every file in this query folder
        for _input_file in input_filepaths:
            with _input_file.open("r", encoding="utf-8") as f:
                result = json.load(f)

            pubmed_abstract_dict = construct_pubmed_abstract_object(result)

            has_abstract: bool = check_if_has_abstract(
                result=pubmed_abstract_dict, tolerance=abstract_size_tolerance
            )

            if has_abstract:
                _each_result_filepath: Path = Path(
                    f"{run_output_filepath_per_query_term}/{str(_success_count).zfill(3)}.json"
                )
                _success_count += 1
            else:
                if not os.path.exists(
                    Path(f"{run_output_filepath_per_query_term}/rejected/")
                ):
                    run_reject_filepath: Path = Path(
                        f"{run_output_filepath_per_query_term}/rejected/"
                    )
                    run_reject_filepath.mkdir(parents=True, exist_ok=True)
                _each_result_filepath: Path = Path(
                    f"{run_reject_filepath}/{str(_reject_count).zfill(3)}.json"
                )
                _reject_count += 1

            with _each_result_filepath.open("w", encoding="utf-8") as f:
                json.dump(pubmed_abstract_dict, f, default=convert_possible_date_to_str)

        # Add processing results metadata after end of processing each folder
        with _processing_metadata_filepath.open("r", encoding="utf-8") as f:
            _loaded_processing_metadata: dict = json.load(f)

        with _processing_metadata_filepath.open("w", encoding="utf-8") as f:
            _loaded_processing_metadata["num_queries_retrieved"] = _success_count
            _loaded_processing_metadata["processing_report"] = {
                "success": _success_count,
                "reject": _reject_count,
            }

            json.dump(_loaded_processing_metadata, f)


def construct_pubmed_abstract_object(pubmed_article: dict) -> dict:
    _pubmed_abstract_dict: dict = {}

    _pubmed_abstract_dict["pmid"] = str(
        pubmed_article["PubmedArticle"][0]["MedlineCitation"]["PMID"]
    )
    if (
        "ELocationID"
        in pubmed_article["PubmedArticle"][0]["MedlineCitation"]["Article"].keys()
    ):
        _pubmed_abstract_dict["doi"] = list(
            map(
                str,
                pubmed_article["PubmedArticle"][0]["MedlineCitation"]["Article"][
                    "ELocationID"
                ],
            )
        )
    if (
        "Language"
        in pubmed_article["PubmedArticle"][0]["MedlineCitation"]["Article"].keys()
    ):
        _pubmed_abstract_dict["language"] = list(
            map(
                str,
                pubmed_article["PubmedArticle"][0]["MedlineCitation"]["Article"][
                    "Language"
                ],
            )
        )
    _pubmed_abstract_dict["title"] = str(
        pubmed_article["PubmedArticle"][0]["MedlineCitation"]["Article"]["ArticleTitle"]
    )
    _pubmed_abstract_dict["abstract"] = pubmed_article["PubmedArticle"][0][
        "MedlineCitation"
    ]["Article"]["Abstract"]["AbstractText"][0]
    if "KeyWordList" in pubmed_article["PubmedArticle"][0]["MedlineCitation"].keys():
        _pubmed_abstract_dict["keywords"] = list(
            map(
                str,
                pubmed_article["PubmedArticle"][0]["MedlineCitation"]["KeywordList"][0],
            )
        )
    if (
        "ArticleDate"
        in pubmed_article["PubmedArticle"][0]["MedlineCitation"]["Article"].keys()
    ) & (
        len(
            pubmed_article["PubmedArticle"][0]["MedlineCitation"]["Article"][
                "ArticleDate"
            ]
        )
        > 1
    ):
        _pubmed_abstract_dict["article_date"] = extract_article_date(
            pubmed_article=pubmed_article, date_struct_name="ArticleDate"
        )
    if "DateCompleted" in pubmed_article["PubmedArticle"][0]["MedlineCitation"].keys():
        _pubmed_abstract_dict["date_completed"] = extract_other_date(
            pubmed_article=pubmed_article, date_struct_name="DateCompleted"
        )
    if "DateRevised" in pubmed_article["PubmedArticle"][0]["MedlineCitation"].keys():
        _pubmed_abstract_dict["date_revised"] = extract_other_date(
            pubmed_article=pubmed_article, date_struct_name="DateRevised"
        )

    # pubmed_abstract_object = PubmedAbstractObject(**_pubmed_abstract_dict)
    # return pubmed_abstract_object
    return _pubmed_abstract_dict


def check_if_has_abstract(result: dict, tolerance: int = 100):
    return len(result.get("abstract", "")) > tolerance


def extract_article_date(pubmed_article: dict, date_struct_name: str) -> date:
    _date_year = pubmed_article["PubmedArticle"][0]["MedlineCitation"]["Article"][
        date_struct_name
    ][0]["Year"]
    _date_month = pubmed_article["PubmedArticle"][0]["MedlineCitation"]["Article"][
        date_struct_name
    ][0]["Month"]
    _date_day = pubmed_article["PubmedArticle"][0]["MedlineCitation"]["Article"][
        date_struct_name
    ][0]["Day"]
    return date(
        year=int(_date_year),
        month=int(_date_month),
        day=int(_date_day),
    )


def extract_other_date(pubmed_article: dict, date_struct_name: str) -> date:
    _date_year = pubmed_article["PubmedArticle"][0]["MedlineCitation"][
        date_struct_name
    ]["Year"]
    _date_month = pubmed_article["PubmedArticle"][0]["MedlineCitation"][
        date_struct_name
    ]["Month"]
    _date_day = pubmed_article["PubmedArticle"][0]["MedlineCitation"][date_struct_name][
        "Day"
    ]
    return date(
        year=int(_date_year),
        month=int(_date_month),
        day=int(_date_day),
    )
