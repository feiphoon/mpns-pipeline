from invoke import task, Collection


@task
def build(c):
    c.run(
        "docker build -t punchy/ner-pipeline:0.1.0 -t punchy/ner-pipeline:latest .",
        pty=True,
    )


@task
def build_no_cache(c):
    c.run(
        "docker build --no-cache -t punchy/ner-pipeline:0.1.0 -t punchy/ner-pipeline:latest .",
        pty=True,
    )


@task
def pyspark_run(c):
    c.run(
        "docker run -v $(pwd):/job punchy/ner-pipeline:0.1.0 /main.py --name 'ner-pipeline-container'",
        pty=True,
    )


ns = Collection()
ps = Collection("ps")

ps.add_task(build)
ps.add_task(build_no_cache)
ps.add_task(pyspark_run, name="run")
ns.add_collection(ps)


@task
def test(c):
    c.run("python -m pytest -vv", pty=True)


@task
def lint(c):
    c.run("python -m flake8 src/.", pty=True)
    c.run("python -m flake8 tests/.", pty=True)
    c.run("python -m black --check .", pty=True)


ns.add_task(test)
ns.add_task(lint)
