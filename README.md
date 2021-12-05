# mpns-pipeline

- [mpns-pipeline](#mpns-pipeline)
  * [Setup & run instructions](#setup---run-instructions)
    + [Installations](#installations)
    + [Run this repo](#run-this-repo)
    + [`inv` task breakdown](#-inv--task-breakdown)
      - [`inv ps.build`](#-inv-psbuild-)
      - [`inv ps.build-no-cache`](#-inv-psbuild-no-cache-)
      - [`inv ps.mpns_v8_processing_run_v5`](#-inv-psmpns-v8-processing-run-v5-)
      - [`inv test`](#-inv-test-)
      - [`inv lint`](#-inv-lint-)
    + [VSCode settings](#vscode-settings-)


This part of the work is to produce static MPNS data for the NER pipeline. This pipeline itself is a one-off pipeline and is not meant to be re-run to produce new data, unless a new version of MPNS is introduced.

The code stages in this repo:
- [A_mpns_v8_processing](src/A_mpns_v8_processing/README.md)
- [B_create_labels_for_annotation](src/B_create_labels_for_annotation/README.md)

The data stages in this repo:
- **A_mpns_v8_processing:** The raw MPNS v8 datasets in `data/mpns/mpns_v8` folder are processed to `processed/mpns/mpns_v8/mpns_name_mappings/v5/scientific_name_type=...`
    The entire repo and pipeline are designed to easily allow a new version of MPNS to be introduced.
- **B_create_labels_for_annotation:** This is a very simple file of labels to be uploaded to the annotation tool, located at `data/reference/mpns_v8/annotation_labels.json`.
- **C_mpns_v8_botanical_name_analysis:**
- **D_mpns_v8_name_relationships_analysis:**
- **E_get_mpns_v8_analyses_answers:**


More details on files are in [TREE.md](TREE.md).

---

## Setup & run instructions

### Installation

First, use `pyenv` to set Python version:
```bash
pyenv update
pyenv install --list
pyenv install 3.10.0
pyenv local 3.10.0
```

Then create a virtual env to run tasks and tests - namely, `invoke` and `pytest` libraries.

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r ops_requirements.txt
...
deactivate
```
OR

```bash
pyenv virtualenv 3.10.0 mpns-pipeline
pyenv local mpns-pipeline
pyenv local
pyenv activate mpns-pipeline
pip install -r ops_requirements.txt
pyenv deactivate mpns-pipeline
```

Install Docker and log in with credentials created on DockerHub.

### Run this repo

The base image is from: <https://hub.docker.com/r/godatadriven/pyspark>.

The execution of all tasks in this repo are simplified using the `invoke` Python library. The available commands can be viewed by running:

```bash
inv --list
```

Anything prefixed with `ps.` is a command namespaced to `ps`, for PySpark, and that will run a PySpark job.

To build the docker image, run:
```bash
inv ps.build
```

To rebuild instead of drawing the base image from cache:
```bash
inv ps.build-no-cache
```

To run the processing (the latest version is v5):
```bash
inv ps.mpns_v8_processing_run_v5
```

### `inv` task breakdown

#### `inv ps.build`

Login to Docker and build the docker image from the Dockerfile.
The following command names the image and tags it.
```bash
docker build -t punchy/mpns-pipeline:0.1.0 .
```

#### `inv ps.build-no-cache`
To rebuild instead of drawing from cache:
```bash
docker build --no-cache -t punchy/mpns-pipeline:0.1.0 .
```

Check that the necessary images were created. The repositories and tags we want are: `punchy/mpns-pipeline, 0.1.0` and `godatadriven/pyspark, 3.0.2-buster`
```bash
docker image ls
```

#### `inv ps.mpns_v8_processing_run_v5`
Make a docker volume, defining a `job` folder on it, and run the processing of v5 on it.

```bash
docker run -v $(pwd):/job punchy/mpns-pipeline:0.1.0 [options] /main.py [app arguments]
```

Or:

```bash
docker run -v $(pwd):/job punchy/mpns-pipeline:0.1.0  \
    --name "mpns-pipeline-volume" \
    --master "local[1]" \
    --conf "spark.ui.showConsoleProgress=True" \
    --conf "spark.ui.enabled=False" \
    /main.py
```

#### `inv test`

Runs pytest in verbose mode.


#### `inv lint`

Runs linting using `flake8` and `black`.


### VSCode settings

Create a `.vscode` folder and put the following into a `settings.json` file inside it.

Make sure this path is added to `.gitignore` (replace with the direct directory for `pythonPath`).

```json
{
    "python.pythonPath": "/Users/fei/.pyenv/versions/mpns-pipeline",
    "python.analysis.extraPaths": [
        "src",
        "tests"
    ],
    "python.terminal.activateEnvironment": true,
    "python.linting.enabled": true,
    "python.linting.pylintEnabled": false,
    "python.linting.flake8Enabled": true,
    "python.linting.flake8Args": [
        "--config",
        ".flake8",
        "--max-line-length=120",
    ],
    "python.formatting.provider": "black",
    "editor.formatOnSave": true,
    "editor.rulers": [
        {
            "column": 80,
            "color": "#34ebb7"
        },
        100,
        {
            "column": 120,
            "color": "#eb34c3"
        },
    ],
}
```
