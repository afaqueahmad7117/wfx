import yaml
from typing import Dict, List, Any


class DBXDabConverter:
    """
    Converts JSON configurations to Databricks Asset Bundle (DAB) YAML format.
    Supports all resource types defined in Databricks documentation.
    """

    def __init__(self):
        self.supported_task_types = {
            "notebook_task",
            "spark_python_task",
            "spark_jar_task",
            "spark_submit_task",
            "pipeline_task",
            "python_wheel_task",
            "sql_task",
            "dbt_task",
            "repos",
        }

        self.cluster_types = {"new_cluster", "existing_cluster_id", "job_cluster_key"}

    def convert_cluster_config(self, cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert cluster configuration to DABs format.
        """
        if not cluster_config:
            return {}

        valid_keys = {
            "num_workers",
            "spark_version",
            "spark_conf",
            "aws_attributes",
            "node_type_id",
            "driver_node_type_id",
            "custom_tags",
            "cluster_log_conf",
            "init_scripts",
            "spark_env_vars",
            "enable_elastic_disk",
            "instance_pool_id",
            "policy_id",
            "driver_instance_pool_id",
            "runtime_engine",
            "data_security_mode",
            "workload_type",
        }

        return {k: v for k, v in cluster_config.items() if k in valid_keys}

    def convert_task_dependencies(
        self, dependencies: List[Dict[str, str]]
    ) -> List[Dict[str, str]]:
        """
        Convert task dependencies to correct DABs format.
        Each dependency should be in format: {"task_key": "task_name"}
        """
        converted_deps = []
        for dep in dependencies:
            if isinstance(dep, dict) and "task_key" in dep:
                # Already in correct format with task_key
                converted_deps.append({"task_key": dep["task_key"]})
            elif isinstance(dep, dict):
                # Convert other dict formats
                task_key = next(iter(dep.values()))  # Get the first value as task key
                converted_deps.append({"task_key": task_key})
            elif isinstance(dep, str):
                # Convert string format
                converted_deps.append({"task_key": dep})
        return converted_deps

    def convert_task_config(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert task configuration to DABs format.
        Handles all supported task types and their specific configurations.
        """
        dabs_task = {"task_key": task["task_key"]}

        if "depends_on" in task:
            dabs_task["depends_on"] = self.convert_task_dependencies(task["depends_on"])

        # Handle cluster specification
        for cluster_type in self.cluster_types:
            if cluster_type in task:
                if cluster_type == "new_cluster":
                    dabs_task[cluster_type] = self.convert_cluster_config(
                        task[cluster_type]
                    )
                else:
                    dabs_task[cluster_type] = task[cluster_type]
                break

        # Handle task-specific configurations
        for task_type in self.supported_task_types:
            if task_type in task:
                dabs_task[task_type] = self.convert_task_specific_config(
                    task_type, task[task_type]
                )
                break

        common_params = {
            "email_notifications",
            "timeout_seconds",
            "max_retries",
            "min_retry_interval_millis",
            "retry_on_timeout",
            "schedule",
            "notification_settings",
            "health",
            "libraries",
        }

        for param in common_params:
            if param in task:
                dabs_task[param] = task[param]

        return dabs_task

    def convert_task_specific_config(
        self, task_type: str, config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Convert task-specific configurations based on task type.
        """
        if task_type == "notebook_task":
            return {
                "notebook_path": config.get("notebook_path", ""),
                "source": config.get("source", "WORKSPACE"),
                **(
                    {"base_parameters": config["base_parameters"]}
                    if config.get("base_parameters")
                    else {}
                ),
            }

        elif task_type == "spark_python_task":
            return {
                "python_file": config["python_file"],
                **(
                    {"parameters": config["parameters"]}
                    if "parameters" in config
                    else {}
                ),
            }

        elif task_type == "spark_jar_task":
            return {
                "main_class_name": config["main_class_name"],
                **(
                    {"parameters": config["parameters"]}
                    if "parameters" in config
                    else {}
                ),
            }

        elif task_type == "spark_submit_task":
            return config

        elif task_type == "pipeline_task":
            return {
                "pipeline_id": config["pipeline_id"],
                **(
                    {"full_refresh": config["full_refresh"]}
                    if "full_refresh" in config
                    else {}
                ),
            }

        elif task_type == "python_wheel_task":
            return {
                "package_name": config["package_name"],
                "entry_point": config["entry_point"],
                **(
                    {"parameters": config["parameters"]}
                    if "parameters" in config
                    else {}
                ),
            }

        elif task_type == "sql_task":
            return {
                "query": config.get("query", ""),
                "warehouse_id": config.get("warehouse_id", ""),
                **(
                    {"parameters": config["parameters"]}
                    if "parameters" in config
                    else {}
                ),
            }

        elif task_type == "dbt_task":
            return {
                "project_directory": config["project_directory"],
                "commands": config["commands"],
                **({"schema": config["schema"]} if "schema" in config else {}),
                **(
                    {"warehouse_id": config["warehouse_id"]}
                    if "warehouse_id" in config
                    else {}
                ),
            }

        return config

    def convert_job_config(
        self, input_json: Dict[str, Any], job_name: str
    ) -> Dict[str, Any]:
        """
        Convert complete job configuration to DABs format.
        """
        dabs_job = {"resources": {"jobs": {job_name: {"name": job_name, "tasks": []}}}}

        tasks = input_json.get("tasks", [])
        for task in tasks:
            dabs_task = self.convert_task_config(task)
            dabs_job["resources"]["jobs"][job_name]["tasks"].append(dabs_task)

        # Handle job clusters if present
        if "job_clusters" in input_json:
            dabs_job["resources"]["jobs"][job_name]["job_clusters"] = [
                {
                    "job_cluster_key": cluster.get("job_cluster_key", f"cluster_{i}"),
                    "new_cluster": self.convert_cluster_config(cluster["new_cluster"]),
                }
                for i, cluster in enumerate(input_json["job_clusters"])
            ]

        # Handle job-level parameters
        job_params = {
            "email_notifications",
            "timeout_seconds",
            "schedule",
            "max_concurrent_runs",
            "git_source",
            "tags",
            "format",
            "access_control_list",
            "notification_settings",
            "health",
            "trigger",
            "run_as",
        }

        for param in job_params:
            if param in input_json:
                dabs_job["resources"]["jobs"][job_name][param] = input_json[param]

        return dabs_job

    def save_dabs_yaml(self, dabs_job: Dict[str, Any], output_path: str):
        """
        Save the DABs job configuration as YAML file.
        """

        class NoAliasDumper(yaml.SafeDumper):
            def ignore_aliases(self, data):
                return True

        def str_presenter(dumper, data):
            if len(data.split("\n")) > 1:
                return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
            return dumper.represent_scalar("tag:yaml.org,2002:str", data)

        yaml.add_representer(str, str_presenter, Dumper=NoAliasDumper)

        with open(output_path, "w") as f:
            yaml.dump(
                dabs_job,
                f,
                default_flow_style=False,
                sort_keys=False,
                Dumper=NoAliasDumper,
            )
