import os
from pathlib import Path
from typing import List


def get_directories(directory: str = ".") -> List:
    return [
        Path(os.path.join(directory, obj))
        for obj in os.listdir(directory)
        if os.path.isdir(os.path.join(directory, obj))
    ]


def get_files(directory: str = ".") -> List:
    return [
        Path(os.path.join(directory, obj))
        for obj in os.listdir(directory)
        if not os.path.isdir(os.path.join(directory, obj))
    ]
