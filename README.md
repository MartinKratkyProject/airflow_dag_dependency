# Airflow DAG Dependencies Scanner

## Description

This Python script scans all DAG files in the `dags/` folder and identifies **dependencies between DAGs**.  
It works by parsing each DAG file and looking for calls to the `TriggerDagRunOperator`.  
Whenever it finds a `trigger_dag_id` argument, it records a dependency between the current DAG and the target DAG.

The script uses Python’s built-in `ast` module to parse code safely and the `TriggerDagRunVisitor` class to traverse the abstract syntax tree (AST) and extract dependencies.

---

## How It Works

1. Scans every `.py` file inside the `dags/` folder.  
2. Extracts the `dag_id` from any `DAG()` declaration.  
3. Searches for `TriggerDagRunOperator(trigger_dag_id=...)` calls.  
4. Prints all dependencies in the format:
    dag1 > dag2
    dag1 > dag3


This means **`dag1` triggers `dag2` and `dag3`.**

---

## Usage
Run the script using Python:

```bash
python dags/utils/scan_dependencies.py
```
If you have your own airflow instance running, you can copy the /dags/utils/scan_dependencies.py file and:
1. Create a new `utils` folder in your dags folder.
2. Paste the `scan_dependencies.py` file in the `utils` folder.

The project is using the official Apache Airflow Docker Compose setup, with environment variables configured via .env and env.example.

# Notes:
The script ignores files that fail to parse safely.
Only dependencies defined using TriggerDagRunOperator are detected.
You can extend the script to support additional operators or relationships.