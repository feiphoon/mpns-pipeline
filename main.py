from src.helpers.run import Run

from src.A_pubmed_abstract_import.biopython_import import import_pubmed_abstracts
from src.B_pubmed_abstract_processing.biopython_processing import (
    process_pubmed_abstracts,
)


if __name__ == "__main__":
    run = Run()

    pubmed_abstracts_raw_filepath = "data/raw/pubmed_abstracts"
    terms = ["Bellis perennis L."]
    for t in terms:
        import_pubmed_abstracts(
            run_filepath=run.create_run_filepath(pubmed_abstracts_raw_filepath),
            query_terms=t,
            num_results=10,
        )

    pubmed_abstracts_processed_filepath = "data/processed/pubmed_abstracts"
    process_pubmed_abstracts(
        run_input_filepath=run.create_run_filepath(pubmed_abstracts_raw_filepath),
        run_output_filepath=run.create_run_filepath(
            pubmed_abstracts_processed_filepath
        ),
        abstract_size_tolerance=100,
    )
