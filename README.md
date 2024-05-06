[![CircleCI](https://circleci.com/gh/LD4P/ils-middleware/tree/main.svg?style=svg)](https://circleci.com/gh/LD4P/ils-middleware/tree/main)

# ILS Middleware
Proof-of-concept for using [Apache Airflow][AF] to manage Sinopia workflows
that interact with institutional integrated library systems (ILS) and/or
library services platform (LSP). Currently there are Directed Acyclic Graphs (DAG)
for Stanford and Cornell Sinopia-to-ILS/LSP workflows. Alma users with the Linked Data API enabled can use the Alma DAG for connecting Sinopia to Alma.

## Running Locally with Docker
Based on the documentation, [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html).

> **NOTE** Make sure there is enough RAM available locally for the
> docker daemon, we recommend at least 5GB.

1. Clone repository `git clone https://github.com/LD4P/ils-middleware`
1. If it's commented out, uncomment the line `- ./dags:/opt/airflow/dags` in docker-compose.yaml (under `volumes`, under `x-airflow-common`).
1. Run `docker compose up airflow-init` to initialize the Airflow
1. Bring up airflow, `docker compose up` to run the containers in the foreground,
   add `docker compose up -d` to run as a daemon.
1. Access Airflow locally at http://localhost:8080

### Setup the SQS queue in localstack for local development

```
AWS_ACCESS_KEY_ID=999999 AWS_SECRET_ACCESS_KEY=1231 aws sqs \
    --endpoint-url=http://localhost:4566 create-queue \
    --region us-west-2 \
    --queue-name all-institutions
```

### Setup the local AWS connection for SQS

1. From the `Admin > Connections` menu
2. Click the "+"
3. Add an Amazon Web Services connection with the following settings:

    * Connection Id: aws_sqs_connection
    * Login: 999999
    * Password: 1231
    * Extra: `{"host": "http://localstack:4566", "region_name": "us-west-2"}`

### Send a message to the SQS queue

In order to test a dag locally, a message must be sent to the above queue:
```
AWS_ACCESS_KEY_ID=999999 AWS_SECRET_ACCESS_KEY=1231 aws sqs \
    send-message \
    --endpoint-url=http://localhost:4566 \
    --queue-url https://localhost:4566/000000000000/all-institutions \
    --message-body file://tests/fixtures/sqs/test-message.json
```

Note: the test message content in `tests/fixtures/sqs/test-message.json` contains an email address that you can update to your own.

## Editing existing DAGs
The `dags/stanford.py` contains Stanford's Symphony and FOLIO workflows from
Sinopia editor user initiated process. The `dags/cornell.py` DAG is for Cornell's
FOLIO workflow. Editing either of these code files will change the DAG.

## Adding a new DAG
In the `dags` subdirectory, add a python module for the DAG. Running Airflow
locally through Docker (see above), the DAG should appear in the list of DAGs
or display any errors in the DAG.

To add any new DAGs to `ld4p/ils-middleware:latest` image, you can either
* build the image locally with `docker build -t ld4p/ils-middleware:latest .` or,
* if commented out, uncomment the `build: .` line (under `x-airflow-common`) in `docker-compose.yaml`
while commenting out the previous line `image: ${AIRFLOW_IMAGE_NAME:-ld4p/ils-middleware:latest}`.

## Dependency Management and Packaging
We are using [poetry][POET] to better manage dependency updates. Installation
instructions can be found at https://python-poetry.org/docs/#osx--linux--bashonwindows-install-instructions

Once you have poetry installed, you can install the other project dependencies
by running `poetry install`

## Automated Tests

The [pytest][PYTEST] framework is used to run the tests.  Tests can be invoked manually by calling `poetry run pytest` (which will save an xml formatted [coverage report][PYTESTCOV], as well as printing the coverage report to the terminal).

## Building Python Package
If you plan on building a local Docker image, be sure to build the Python
installation wheel first by running `poetry build`.

## Typechecking

The [mypy][MYPY] static type checker is used to find type errors (parameter passing, assignment, return, etc of incompatible value types).  CI runs `poetry run mypy --ignore-missing-imports .` (as not all imports have type info available).

## Linting

The [flake8][FLK8] Python code linter can be manually run by invoking `poetry run flake8` from
the command-line. Configuration options are in the `setup.cfg` file, under the flake8 section.

## Code formatting

Code can be auto-formatted using [Black][BLACK], an opinionated Python code formatter.

To see whether Black would make changes: `poetry run black --check .`

To have Black apply formatting: `poetry run black .`

For information about integrations, including Git hooks and plugins for popular IDEs, see:  https://black.readthedocs.io/en/stable/integrations/index.html

[AF]: https://airflow.apache.org/
[BLACK]: https://black.readthedocs.io/
[FLK8]: https://flake8.pycqa.org/en/latest/
[POET]: https://python-poetry.org/
[PYTEST]: https://docs.pytest.org/
[PYTESTCOV]: https://github.com/pytest-dev/pytest-cov
[MYPY]: https://mypy.readthedocs.io/en/stable/
