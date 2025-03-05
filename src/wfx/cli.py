import click
from pathlib import Path
from wfx.core.converter import DBXDabConverter

PROJECT_ROOT = Path(__file__).parent.parent.parent


@click.command()
@click.option("--input-dir", "-i", required=True, help="Input Airflow DAG folder path")
@click.option("--dag-name", "-d", required=True, help="Input DAG folder name")
@click.option(
    "--output-dir", "-o", required=True, help="Output workflow folder path"
)
@click.option("--workflow-name", "-w", required=True, help="Output workflow file name")
@click.option("--config", "-c", required=True, help="Config file path")
def main(
    input_dir: str,
    dag_name: str,
    output_dir: str,
    workflow_name: str,
    config: str,
):
    """Convert JSON job configuration to DABs YAML format."""

    dag_path = Path(input_dir)
    workflow_path = Path(output_dir)
    config_path = Path(config)

    if not dag_path.is_absolute():
        dag_path = PROJECT_ROOT / dag_path

    if not config_path.is_absolute():
        config_path = PROJECT_ROOT / config

    from src.wfx.processors.gsheet import GsheetProcessor
    from src.wfx.processors.airflow import AirflowProcessor

    gsheet_processor = GsheetProcessor(dag_name, str(config_path))
    airflow_processor = AirflowProcessor(dag_name, gsheet_processor, str(dag_path))
    target_tasks = airflow_processor.build_save_target_tasks()

    converter = DBXDabConverter()
    dabs_job = converter.convert_job_config(target_tasks, workflow_name)

    if not workflow_path.is_absolute():
        workflow_path = PROJECT_ROOT / workflow_path

    write_path = str(workflow_path / workflow_name) + ".yml"
    converter.save_dabs_yaml(dabs_job, str(write_path))
    click.echo(f"Successfully created DABs job configuration: {write_path}")


if __name__ == "__main__":
    main(
        standalone_mode=False,
        args=[
            "-i",
            "inputs/airflow_dags",
            "-d",
            "data_processing",
            "-o",
            "resources/jobs",
            "-w",
            "sample_databricks_etl",
            "-c",
            "inputs/mappings/data_processing/task_list_all.csv",
        ],
    )
