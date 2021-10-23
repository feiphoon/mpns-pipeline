"""
Using the biopython.Bio library - example script here:
https://github.com/biopython/biopython/blob/master/Scripts/query_pubmed.py

More:
https://towardsdatascience.com/write-a-document-classifier-in-less-than-30-minutes-2d96a8a8820c
"""
import json
from pathlib import Path
from Bio import Entrez
from typing import List

from src.helpers.preprocessing import snakify_text


Entrez.email = "my@email.address"
Entrez.tool = "biopython"


def import_pubmed_abstracts(
    run_filepath: Path,
    query_terms: str,
    num_results: int,
    pubmed_login: Entrez = Entrez,
) -> None:
    run_filepath.mkdir(parents=True, exist_ok=True)
    run_filepath_per_query_term: Path = Path(
        f"{run_filepath}/{snakify_text(query_terms)}"
    )
    run_filepath_per_query_term.mkdir(parents=True, exist_ok=True)

    query_results = search(pubmed_login, query_terms, num_results)
    query_id_list: List = query_results.get("IdList", [])
    _query_metadata_filepath: Path = Path(
        f"{run_filepath_per_query_term}/query_metadata.json"
    )

    with _query_metadata_filepath.open("w", encoding="utf-8") as f:
        _query_metadata: dict = {}
        _query_metadata["query_terms"] = query_terms
        _query_metadata["num_results_requested"] = num_results
        _query_metadata["query_id_list"] = query_id_list
        json.dump(_query_metadata, f)

    _count: int = 0

    for query_id in query_id_list:
        pubmed_abstract_dict = retrieve_results(query_id)

        _each_result_filepath: Path = Path(
            f"{run_filepath_per_query_term}/{str(_count).zfill(3)}.json"
        )
        _count += 1

        with _each_result_filepath.open("w", encoding="utf-8") as f:
            json.dump(pubmed_abstract_dict, f)

    # Add query results metadata after end of retrieval
    with _query_metadata_filepath.open("r", encoding="utf-8") as f:
        _loaded_query_metadata: dict = json.load(f)

    with _query_metadata_filepath.open("w", encoding="utf-8") as f:
        _loaded_query_metadata["num_queries_retrieved"] = _count
        json.dump(_loaded_query_metadata, f)


def search(pubmed_login: Entrez, query: str, max_documents: int = 100) -> list:
    handle = pubmed_login.esearch(
        db="pubmed", sort="relevance", retmax=max_documents, retmode="xml", term=query
    )
    return pubmed_login.read(handle)


def retrieve_results(id: str) -> dict:
    handle = Entrez.efetch(
        db="pubmed",
        id=id,
        retmode="xml",
    )
    return Entrez.read(handle)
