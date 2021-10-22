# mpns-pipeline

## Setup

Recommendations:
pyenv to run python
```bash
pyenv update
pyenv install --list
pyenv install 3.10.0
pyenv local 3.10.0
```

Virtual env to run tasks and tests:

Namely, `invoke` and `pytest` libraries.

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

The base image is from: <https://hub.docker.com/r/godatadriven/pyspark>.

Login to Docker and build the docker image from the Dockerfile.
The following command names the image and tags it.
```bash
docker build -t punchy/mpns-pipeline:0.1.0 .
```

To rebuild instead of drawing from cache:
```bash
docker build --no-cache -t punchy/mpns-pipeline:0.1.0 .
```

Check that the necessary images were created. The repositories and tags we want are: `punchy/mpns-pipeline, 0.1.0` and `godatadriven/pyspark, 3.0.2-buster`
```bash
docker image ls
```

Make a docker volume, defining a `job` folder on it, and run the file on it

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

## VSCode settings:

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