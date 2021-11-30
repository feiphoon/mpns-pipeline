from invoke import task, Collection


@task
def build(c):
    c.run(
        "docker build -t punchy/mpns-pipeline:0.1.0 -t punchy/mpns-pipeline:latest .",
        pty=True,
    )


@task
def build_no_cache(c):
    c.run(
        "docker build --no-cache -t punchy/mpns-pipeline:0.1.0 -t punchy/mpns-pipeline:latest .",
        pty=True,
    )


@task
def mpns_v8_processing_run_v1(c):
    c.run(
        "docker run -v $(pwd):/job punchy/mpns-pipeline:0.1.0 \
            src/A_mpns_v8_processing/mpns_v8_processing_v1.py \
            --name 'mpns-pipeline-container'\
                ;CONTAINER_ID=$(docker ps -lq)\
                    ;docker cp `echo $CONTAINER_ID`:/data/processed/mpns/ data/processed/",
        pty=True,
    )


@task
def mpns_v8_processing_run_v2(c):
    c.run(
        "docker run -v $(pwd):/job punchy/mpns-pipeline:0.1.0 \
            src/A_mpns_v8_processing/mpns_v8_processing_v2.py \
            --name 'mpns-pipeline-container'\
                ;CONTAINER_ID=$(docker ps -lq)\
                    ;docker cp `echo $CONTAINER_ID`:/data/processed/mpns data/processed/",
        pty=True,
    )


@task
def mpns_v8_processing_run_v3(c):
    c.run(
        "docker run -v $(pwd):/job punchy/mpns-pipeline:0.1.0 \
            src/A_mpns_v8_processing/mpns_v8_processing_v3.py \
            --name 'mpns-pipeline-container'\
                ;CONTAINER_ID=$(docker ps -lq)\
                    ;docker cp `echo $CONTAINER_ID`:/data/processed/mpns data/processed/",
        pty=True,
    )


@task
def mpns_v8_processing_run_v4(c):
    c.run(
        "docker run -v $(pwd):/job punchy/mpns-pipeline:0.1.0 \
            src/A_mpns_v8_processing/mpns_v8_processing_v4.py \
            --name 'mpns-pipeline-container'\
                ;CONTAINER_ID=$(docker ps -lq)\
                    ;docker cp `echo $CONTAINER_ID`:/data/processed/mpns data/processed/",
        pty=True,
    )


@task
def mpns_v8_name_mappings_analysis_v3(c):
    c.run(
        "docker run -v $(pwd):/job punchy/mpns-pipeline:0.1.0 \
            src/D_mpns_v8_botanical_name_analysis/analyse_mpns_v8_name_mappings_v3.py \
            --name 'mpns-pipeline-container'\
                ;CONTAINER_ID=$(docker ps -lq)\
                    ;docker cp `echo $CONTAINER_ID`:/data/analysis/mpns data/analysis/",
        pty=True,
    )


@task
def mpns_v8_name_relationships_analysis(c):
    c.run(
        "docker run -v $(pwd):/job punchy/mpns-pipeline:0.1.0 \
            src/E_get_simplest_mpns_v8_name_relationships/analyse_mpns_v8_name_relationships.py \
            --name 'mpns-pipeline-container'\
                ;CONTAINER_ID=$(docker ps -lq)\
                    ;docker cp `echo $CONTAINER_ID`:/data/analysis/mpns data/analysis/",
        pty=True,
    )


ns = Collection()
ps = Collection("ps")

ps.add_task(build)
ps.add_task(build_no_cache)
ps.add_task(mpns_v8_processing_run_v1)
ps.add_task(mpns_v8_processing_run_v2)
ps.add_task(mpns_v8_processing_run_v3)
ps.add_task(mpns_v8_processing_run_v4)
ps.add_task(mpns_v8_name_mappings_analysis_v3, "mpns_name_mappings_analysis")
ps.add_task(mpns_v8_name_relationships_analysis, "mpns_v8_name_relationships_analysis")
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
