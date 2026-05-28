# import json
# import subprocess
# from datetime import datetime
# from pathlib import Path

# from airflow.decorators import dag, task
# from airflow.operators.empty import EmptyOperator
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator


# DBT_PROJECT_DIR = "/opt/airflow/dbt/olist_warehouse"
# DBT_PROFILES_DIR = "/opt/airflow/dbt/olist_warehouse"
# MANIFEST_PATH = "/opt/airflow/dbt/manifest.json"
# DBT_TARGET = "prod"


# LAYER_CONFIG = {
#     "staging": {
#         "dag_id": "dbt_olist_staging",
#         "path_keyword": "models/staging/",
#         "schedule": "@daily",
#         "next_dag": "dbt_olist_intermediate",
#     },
#     "intermediate": {
#         "dag_id": "dbt_olist_intermediate",
#         "path_keyword": "models/intermediate/",
#         "schedule": None,
#         "next_dag": "dbt_olist_core",
#     },
#     "core": {
#         "dag_id": "dbt_olist_core",
#         "path_keyword": "models/marts/core/",
#         "schedule": None,
#         "next_dag": "dbt_olist_analytics",
#     },
#     "analytics": {
#         "dag_id": "dbt_olist_analytics",
#         "path_keyword": "models/marts/analytics/",
#         "schedule": None,
#         "next_dag": None,
#     },
# }


# def load_manifest():
#     manifest_file = Path(MANIFEST_PATH)

#     if not manifest_file.exists():
#         raise FileNotFoundError(
#             f"manifest.json not found at {MANIFEST_PATH}. "
#             "Generate it first with: dbt parse or dbt compile."
#         )

#     with manifest_file.open("r") as f:
#         return json.load(f)


# def get_layer_models(manifest, path_keyword):
#     models = {}

#     for unique_id, node in manifest["nodes"].items():
#         if node.get("resource_type") != "model":
#             continue

#         original_file_path = node.get("original_file_path", "")

#         if path_keyword in original_file_path:
#             models[unique_id] = {
#                 "unique_id": unique_id,
#                 "name": node["name"],
#                 "depends_on": node.get("depends_on", {}).get("nodes", []),
#                 "original_file_path": original_file_path,
#             }

#     return models


# def get_terminal_model_ids(layer_models):
#     """
#     Terminal model = model in this layer that is not a parent
#     of another model in the same layer.
#     """
#     layer_model_ids = set(layer_models.keys())
#     parent_ids_inside_layer = set()

#     for model in layer_models.values():
#         for parent_id in model["depends_on"]:
#             if parent_id in layer_model_ids:
#                 parent_ids_inside_layer.add(parent_id)

#     terminal_model_ids = layer_model_ids - parent_ids_inside_layer

#     return terminal_model_ids


# def create_dbt_layer_dag(layer_name, config):

#     @dag(
#         dag_id=config["dag_id"],
#         start_date=datetime(2026, 5, 25),
#         schedule=config["schedule"],
#         catchup=False,
#         max_active_tasks=2,
#         tags=["dbt", "olist", layer_name],
#     )
#     def dbt_layer_dag():

#         manifest = load_manifest()
#         layer_models = get_layer_models(manifest, config["path_keyword"])
#         terminal_model_ids = get_terminal_model_ids(layer_models)

#         start = EmptyOperator(task_id="start")
#         finish = EmptyOperator(task_id="finish")

#         @task(task_id="dbt_deps")
#         def dbt_deps():
#             subprocess.run(
#                 [
#                     "dbt",
#                     "deps",
#                     "--profiles-dir",
#                     DBT_PROFILES_DIR,
#                 ],
#                 cwd=DBT_PROJECT_DIR,
#                 check=True,
#             )

#         @task(task_id="dbt_seed")
#         def dbt_seed():
#             subprocess.run(
#                 [
#                     "dbt",
#                     "seed",
#                     "--profiles-dir",
#                     DBT_PROFILES_DIR,
#                     "--target",
#                     DBT_TARGET,
#                 ],
#                 cwd=DBT_PROJECT_DIR,
#                 check=True,
#             )

#         @task
#         def run_model(model_name: str):
#             subprocess.run(
#                 [
#                     "dbt",
#                     "run",
#                     "--select",
#                     model_name,
#                     "--profiles-dir",
#                     DBT_PROFILES_DIR,
#                     "--target",
#                     DBT_TARGET,
#                 ],
#                 cwd=DBT_PROJECT_DIR,
#                 check=True,
#             )

#         @task
#         def test_model(model_name: str):
#             subprocess.run(
#                 [
#                     "dbt",
#                     "test",
#                     "--select",
#                     model_name,
#                     "--profiles-dir",
#                     DBT_PROFILES_DIR,
#                     "--target",
#                     DBT_TARGET,
#                 ],
#                 cwd=DBT_PROJECT_DIR,
#                 check=True,
#             )

#         deps_task = dbt_deps()
#         seed_task = dbt_seed()

#         start >> deps_task >> seed_task

#         run_tasks = {}
#         test_tasks = {}

#         for model_id, model in layer_models.items():
#             model_name = model["name"]

#             run_task = run_model.override(
#                 task_id=f"run_{model_name}"
#             )(model_name)

#             test_task = test_model.override(
#                 task_id=f"test_{model_name}"
#             )(model_name)

#             run_task >> test_task

#             run_tasks[model_id] = run_task
#             test_tasks[model_id] = test_task

#         # Dependency dari manifest untuk model dalam layer yang sama
#         for model_id, model in layer_models.items():
#             current_run_task = run_tasks[model_id]

#             internal_parent_found = False

#             for parent_id in model["depends_on"]:
#                 if parent_id in test_tasks:
#                     test_tasks[parent_id] >> current_run_task
#                     internal_parent_found = True

#             # Kalau model tidak punya parent internal di layer ini,
#             # mulai setelah seed.
#             if not internal_parent_found:
#                 seed_task >> current_run_task

#         # Semua terminal model selesai test dulu, baru finish.
#         for terminal_model_id in terminal_model_ids:
#             test_tasks[terminal_model_id] >> finish

#         # Trigger DAG berikutnya setelah semua terminal test selesai.
#         if config["next_dag"]:
#             trigger_next = TriggerDagRunOperator(
#                 task_id=f"trigger_{config['next_dag']}",
#                 trigger_dag_id=config["next_dag"],
#                 wait_for_completion=False,
#                 reset_dag_run=False,
#             )

#             finish >> trigger_next

#     return dbt_layer_dag()


# for layer_name, config in LAYER_CONFIG.items():
#     create_dbt_layer_dag(layer_name, config)
