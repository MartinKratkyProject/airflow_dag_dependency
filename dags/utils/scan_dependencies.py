import os
import ast

UTILS_DIR = os.path.dirname(os.path.abspath(__file__))
DAGS_FOLDER = os.path.dirname(UTILS_DIR)

class TriggerDagRunVisitor(ast.NodeVisitor):
    def __init__(self, dag_id):
        self.dag_id = dag_id
        self.dependencies = []

    def visit_Call(self, node):
        if isinstance(node.func, ast.Name) and node.func.id == "TriggerDagRunOperator":
            for kw in node.keywords:
                if kw.arg == "trigger_dag_id" and isinstance(kw.value, ast.Constant):
                    target_dag = kw.value.value
                    self.dependencies.append((self.dag_id, target_dag))
        elif isinstance(node.func, ast.Attribute) and node.func.attr == "TriggerDagRunOperator":
            for kw in node.keywords:
                if kw.arg == "trigger_dag_id" and isinstance(kw.value, ast.Constant):
                    target_dag = kw.value.value
                    self.dependencies.append((self.dag_id, target_dag))
        self.generic_visit(node)

def get_dag_id(tree):
    dag_id = None
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id == "DAG":
                for kw in node.keywords:
                    if kw.arg == "dag_id" and isinstance(kw.value, ast.Constant):
                        dag_id = kw.value.value
                        return dag_id
                if node.args:
                    first_arg = node.args[0]
                    if isinstance(first_arg, ast.Constant):
                        dag_id = first_arg.value
                        return dag_id
            elif isinstance(node.func, ast.Attribute) and node.func.attr == "DAG":
                for kw in node.keywords:
                    if kw.arg == "dag_id" and isinstance(kw.value, ast.Constant):
                        dag_id = kw.value.value
                        return dag_id
                if node.args:
                    first_arg = node.args[0]
                    if isinstance(first_arg, ast.Constant):
                        dag_id = first_arg.value
                        return dag_id
    return dag_id


def scan_dags(dags_folder):
    all_dependencies = []
    for file in os.listdir(dags_folder):
        if file.endswith(".py") and file != os.path.basename(__file__):
            path = os.path.join(dags_folder, file)
            with open(path, "r", encoding="utf-8") as f:
                source = f.read()
            try:
                tree = ast.parse(source)
                dag_id = get_dag_id(tree)
                if dag_id:
                    visitor = TriggerDagRunVisitor(dag_id)
                    visitor.visit(tree)
                    all_dependencies.extend(visitor.dependencies)
            except Exception as e:
                print(f"Could not parse {file}: {e}")
    return all_dependencies

if __name__ == "__main__":
    deps = scan_dags(DAGS_FOLDER)
    for src, target in deps:
        print(f"{src} > {target}")
