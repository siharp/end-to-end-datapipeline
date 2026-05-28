import json
import os
import re
import shutil
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import get_current_context
from notifications.telegram import notify_failure, notify_success

# =========================
# GLOBAL CONFIG
# =========================

DBT_PROJECT_DIR = "/opt/airflow/dbt/olist_warehouse"
DBT_PROFILES_DIR = "/opt/airflow/dbt/olist_warehouse"
DBT_TARGET = "prod"

# Manifest bersih/stabil yang dibaca DAG layer.
# Jangan baca langsung dari target/manifest.json runtime dbt.
MANIFEST_CLEAR_DIR = "/opt/airflow/dbt_artifacts/olist_warehouse"
MANIFEST_CLEAR_PATH = f"{MANIFEST_CLEAR_DIR}/manifest.json"

# Target path runtime terpisah agar dbt run/test tidak menimpa manifest clear.
DBT_RUNTIME_TARGET_BASE = "/tmp/dbt_runtime_targets/olist_warehouse"

# Target path khusus compile manifest.
DBT_COMPILE_TARGET_BASE = "/tmp/dbt_compile_targets/olist_warehouse"


LAYER_CONFIG = {
    "staging": {
        "dag_id": "dbt_olist_staging",
        "path_keyword": "models/staging/",
        "schedule": None,
        "next_dag": "dbt_olist_intermediate",
    },
    "intermediate": {
        "dag_id": "dbt_olist_intermediate",
        "path_keyword": "models/intermediate/",
        "schedule": None,
        "next_dag": "dbt_olist_core",
    },
    "core": {
        "dag_id": "dbt_olist_core",
        "path_keyword": "models/marts/core/",
        "schedule": None,
        "next_dag": "dbt_olist_analytics",
    },
    "analytics": {
        "dag_id": "dbt_olist_analytics",
        "path_keyword": "models/marts/analytics/",
        "schedule": None,
        "next_dag": None,
    },
}

default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': notify_failure,
}

# =========================
# HELPER FUNCTIONS
# =========================


def sanitize_path_part(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "_", value)


def run_dbt_command(command: list[str], target_path: str) -> None:
    full_command = command + [
        "--profiles-dir",
        DBT_PROFILES_DIR,
        "--target",
        DBT_TARGET,
        "--target-path",
        target_path,
    ]

    subprocess.run(
        full_command,
        cwd=DBT_PROJECT_DIR,
        check=True,
    )


def load_clear_manifest() -> Dict[str, Any]:
    manifest_file = Path(MANIFEST_CLEAR_PATH)

    if not manifest_file.exists():
        # Jangan bikin Broken DAG.
        # DAG tetap muncul, tapi task layer akan kosong/placeholder
        # sampai compile manifest DAG dijalankan.
        return {}

    raw = manifest_file.read_text()

    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"Invalid clear manifest at {MANIFEST_CLEAR_PATH}. "
            f"JSON error: {e}. "
            "Run dbt_olist_compile_manifest to regenerate a clean manifest."
        )


def get_layer_models(manifest: Dict[str, Any], path_keyword: str) -> Dict[str, Dict[str, Any]]:
    models = {}

    if not manifest:
        return models

    for unique_id, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") != "model":
            continue

        original_file_path = node.get("original_file_path", "")

        if path_keyword in original_file_path:
            models[unique_id] = {
                "unique_id": unique_id,
                "name": node["name"],
                "depends_on": node.get("depends_on", {}).get("nodes", []),
                "original_file_path": original_file_path,
            }

    return models


def make_runtime_target_path() -> str:
    context = get_current_context()

    dag_id = sanitize_path_part(context["dag"].dag_id)
    task_id = sanitize_path_part(context["task"].task_id)
    run_id = sanitize_path_part(context["run_id"])

    target_path = f"{DBT_RUNTIME_TARGET_BASE}/{dag_id}/{run_id}/{task_id}"
    Path(target_path).mkdir(parents=True, exist_ok=True)

    return target_path


# =========================
# DAG 0: COMPILE MANIFEST
# =========================

@dag(
    dag_id="dbt_olist_compile_manifest",
    start_date=datetime(2026, 5, 25),
    schedule="@daily",
    catchup=False,
    on_success_callback=notify_success,
    tags=["dbt", "olist", "manifest"],
)
def dbt_olist_compile_manifest():

    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    @task
    def dbt_deps() -> None:
        subprocess.run(
            [
                "dbt",
                "deps",
                "--profiles-dir",
                DBT_PROFILES_DIR,
            ],
            cwd=DBT_PROJECT_DIR,
            check=True,
        )

    @task
    def dbt_seed() -> None:
        context = get_current_context()
        run_id = sanitize_path_part(context["run_id"])
        target_path = f"{DBT_COMPILE_TARGET_BASE}/{run_id}/seed"

        Path(target_path).mkdir(parents=True, exist_ok=True)

        run_dbt_command(
            ["dbt", "seed"],
            target_path=target_path,
        )

    @task
    def dbt_compile_and_copy_manifest() -> None:
        context = get_current_context()
        run_id = sanitize_path_part(context["run_id"])

        compile_target_path = f"{DBT_COMPILE_TARGET_BASE}/{run_id}/compile"
        Path(compile_target_path).mkdir(parents=True, exist_ok=True)

        run_dbt_command(
            ["dbt", "compile"],
            target_path=compile_target_path,
        )

        src_manifest = Path(compile_target_path) / "manifest.json"

        if not src_manifest.exists():
            raise FileNotFoundError(
                f"dbt compile finished but manifest.json was not found at {src_manifest}"
            )

        # Validate JSON before publishing.
        raw = src_manifest.read_text()
        try:
            json.loads(raw)
        except json.JSONDecodeError as e:
            raise ValueError(
                f"Generated manifest is invalid JSON: {e}"
            )

        clear_dir = Path(MANIFEST_CLEAR_DIR)
        clear_dir.mkdir(parents=True, exist_ok=True)

        final_manifest = Path(MANIFEST_CLEAR_PATH)
        tmp_manifest = clear_dir / "manifest.json.tmp"

        shutil.copyfile(src_manifest, tmp_manifest)

        # Atomic replace: prevents Airflow scheduler from reading a half-written file.
        os.replace(tmp_manifest, final_manifest)

    trigger_staging = TriggerDagRunOperator(
        task_id="trigger_dbt_olist_staging",
        trigger_dag_id="dbt_olist_staging",
        wait_for_completion=False,
        reset_dag_run=False,
    )

    deps = dbt_deps()
    seed = dbt_seed()
    compile_copy = dbt_compile_and_copy_manifest()

    start >> deps >> seed >> compile_copy >> finish >> trigger_staging


dbt_olist_compile_manifest()


# =========================
# LAYER DAG FACTORY
# =========================

def create_layer_dag(layer_name: str, config: Dict[str, Any]):

    @dag(
        dag_id=config["dag_id"],
        start_date=datetime(2026, 5, 25),
        schedule=config["schedule"],
        catchup=False,
        max_active_tasks=3,
        on_success_callback=notify_success,
        tags=["dbt", "olist", layer_name],
    )
    def dbt_layer_dag():

        manifest = load_clear_manifest()
        layer_models = get_layer_models(manifest, config["path_keyword"])

        start = EmptyOperator(task_id="start")
        finish = EmptyOperator(task_id="finish")

        @task
        def manifest_missing_or_empty() -> None:
            raise FileNotFoundError(
                f"No models found for layer '{layer_name}'. "
                f"Manifest may be missing or stale: {MANIFEST_CLEAR_PATH}. "
                "Run DAG dbt_olist_compile_manifest first."
            )

        @task
        def run_model(model_name: str) -> None:
            target_path = make_runtime_target_path()

            run_dbt_command(
                ["dbt", "run", "--select", model_name],
                target_path=target_path,
            )

        @task
        def test_model(model_name: str) -> None:
            target_path = make_runtime_target_path()

            run_dbt_command(
                ["dbt", "test", "--select", model_name],
                target_path=target_path,
            )

        # Kalau manifest belum ada saat scheduler parse,
        # DAG tetap valid dan memberi error task yang jelas.
        if not layer_models:
            missing = manifest_missing_or_empty()
            start >> missing >> finish
        else:
            run_tasks = {}
            test_tasks = {}

            for model_id, model in layer_models.items():
                model_name = model["name"]

                run_task = run_model.override(
                    task_id=f"run_{model_name}"
                )(model_name)

                test_task = test_model.override(
                    task_id=f"test_{model_name}"
                )(model_name)

                run_task >> test_task

                run_tasks[model_id] = run_task
                test_tasks[model_id] = test_task

            # Build dependency antar model dalam layer yang sama
            # berdasarkan depends_on.nodes dari manifest.
            for model_id, model in layer_models.items():
                current_run_task = run_tasks[model_id]
                has_internal_parent = False

                for parent_id in model["depends_on"]:
                    if parent_id in test_tasks:
                        test_tasks[parent_id] >> current_run_task
                        has_internal_parent = True

                # Model tanpa parent internal mulai dari start.
                # Parent dari layer sebelumnya dianggap sudah selesai
                # karena antar DAG dikontrol TriggerDagRun.
                if not has_internal_parent:
                    start >> current_run_task

            # Finish menunggu semua test model dalam layer.
            for test_task in test_tasks.values():
                test_task >> finish

        if config["next_dag"]:
            trigger_next = TriggerDagRunOperator(
                task_id=f"trigger_{config['next_dag']}",
                trigger_dag_id=config["next_dag"],
                wait_for_completion=False,
                reset_dag_run=False,
            )

            finish >> trigger_next

    return dbt_layer_dag()


# Generate 4 layer DAGs
for _layer_name, _config in LAYER_CONFIG.items():
    globals()[_config["dag_id"]] = create_layer_dag(_layer_name, _config)
