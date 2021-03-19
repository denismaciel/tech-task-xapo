# Xapo Data Engineer Technincal Challenge

Author: Denis Maciel

## Batch Pipeline

The code related to the batch pipeline is in `batch`. I have used Spark as the
execution engine. More specificaly, I have ingested the raw CSV files into Spark
DataFrames and created temporary views out of them, so that I could write the
data transformations in SQL.

To execute the pipeline steps in the right order, I have used
[Prefect](https://docs.prefect.io/), a pure Python, lightweight
data orchestrator. It adds minimal overhead to get started and seems to scale
well once pipelines get more complex.

To get Spark up and running quicly, I created a custom Dockerimage based off
`jupyter/pyspark-notebook`. In order to run the batch pipeline, you first need
to build the Dockerimage:

```bash
docker build --file batch/Dockerfile --tag xapo-batch batch
```

Once the Dockerimage is built, you can then execute the batch pipeline by
running:

```bash
docker run --rm xapo-batch
```

This command will execute a PySpark job in a Docker container and will print out
to the terminal the first 10 rows of the resulting DataFrames, each of which
contains the solution for a task in this technical challenge.

You can find below the tasks and the SQL queries that solves them. Most of the
discussions and assumptions about the data can be found as comments in the query
files.

### Task 1

> Provide the following information for each card: order date, issue date,
activation date, first used date.

Solved in `batch/queries/card_info.sql`.


### Task 2

> Provide the list of cards which took more than 3 days to be activated and more
than 10 days to be used for the first time. Include the corresponding dates.

Solved in `batch/queries/card_usage.sql`.

This task uses the results of Task 1.

### Task 3

> Provide the list of canceled cards and how many days each of them was active.

Solved in `batch/queries/canceled_cards.sql`.

### Task 4 & 5

> Task 4: Provide the list of merchants together with the number of transactions they had.
> Task 5: Provide the transaction counts by status and by merchant.

Solved in `batch/queries/tx_per_merchant.sql`.

Task 4 and 5 are very similar and I have solved them in the same query.

### Task 6

> Provide the list of top 10 highest spending cards.

Solved in `batch/queries/top10_cards.sql`.

Transaction amounts are given in different currencies. To make them comparable,
I have converted all transaction amounts to USD by using data from [ECB's
Foreign Exchange Rate API](https://api.exchangeratesapi.io/latest/). For the
sake of simplicity, I am using the latest exchange rate for all transactions
regardless of when they happened.  Depending on the use that might be good
enough. If necessary, the exchange rate data can be extended to include exchange
rate for each day. It will just take longer to fetch it.

## Real-time Pipeline

The code related to the real-time pipeline is in `stream`.

The pipeline works as follows. There is a worker (`stream/worker.py`) that
queries and processes the API with information about the latest block of
Bitcoin's blockchain. It then sends the processed information via sockets to a
server (`stream/server.py`). The server is responsible for deciding what to do
with the processed info. In this toy example, it just prints some statistics to
the terminal. In a real-life scenario, the server could, for example, persist
the info in a database or notify other systems if any anomaly is detected etc.

Server and worker run in two different Python processes and are implemented in
pure Python (no need to install dependencies with `pip`).

To start the application, run the following two commands in two different
terminal windows. The server must be started before the worker.

```bash
# In terminal one
python3 stream/server.py

# In terminal two
python3 stream/worker.py
```

Once this is done, you should start seeing information in the terminal about the
data being pulled from the API.

To check the validity of Bitcoin addresses in each transaction, I vendored
the package [base58](https://github.com/keis/base58) into my code base in order
to avoid the need to pip-install it. The whole is in `stream/base58.py`.

## Tools

A brief overview of tools I have used:

* git for version control
* pre-commit for code quality
    * black: code formatting
    * flake8: linter
    * mypy: static type checker
