import re
import unicodedata


def snakify_text(text: str) -> str:
    """This is to transform a text string so it can be used as a folder name.

    The 4 normalisation forms:
    https://en.wikipedia.org/wiki/Unicode_equivalence#Normalization
    """
    result: str = text.lower().strip()
    result = re.sub(r"[\\/&]", "", result)
    result = re.sub(r"[,.]", " ", result)
    result = re.sub(r"[()]", "", result)
    result = result.strip()
    result = re.sub(r"[\s]+", " ", result)
    result = re.sub(r"[\s-]", "_", result)
    result = re.sub(r"[\"'`‘’“”#?!|:;]", "", result)
    result = (
        unicodedata.normalize("NFKD", result).encode("ascii", "ignore").decode("ascii")
    )
    return result
