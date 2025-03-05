from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
print(f"PROJECT_ROOT: {PROJECT_ROOT}")

# Folder Path where Airflow DAGS are stored; Add in PROJECT_ROOT
TARGET_TASKS_PATH = f"{PROJECT_ROOT}/inputs/dbx_workflows"

# Folder path where scripts / notebooks are stored
REPOS_PREFIX = ""

# DABs compatible .yml output folder path; Add in PROJECT_ROOT
TARGET_DAB_YML_PATH = (
    f"{PROJECT_ROOT}/resources/jobs"
)