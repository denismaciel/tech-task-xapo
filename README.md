# Xapo Data Engineer Technincal Challenge

Author: Denis Maciel

## Batch Pipeline

The code related to the batch pipeline is in `batch`.

### Task 1

> Provide the following information for each card: order date, issue date,
activation date, first used date.

### Task 2

> Provide the list of cards which took more than 3 days to be activated and more
than 10 days to be used for the first time. Include the corresponding dates.

### Task 3

> Provide the list of canceled cards and how many days each of them was active.

### Task 4

> Provide the list of merchants together with the number of transactions they had.

### Task 5

> Provide the transaction counts by status and by merchant.

### Task 6

> Provide the list of top 10 highest spending cards.

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

* git for version control
* pre-commit for code quality
    * black: code formatting
    * flake8: linter
    * mypy: static type checker
