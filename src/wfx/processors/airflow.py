import json
import os
import re
import warnings

from wfx.constants.settings import REPOS_PREFIX, TARGET_TASKS_PATH


class AirflowProcessor:
    def __init__(self, workflow_name, gsheet_processor, workflow_setting_path):
        self.workflow_name = workflow_name.lower()
        self.gsheet_processor = gsheet_processor
        self.workflow_setting_path = workflow_setting_path

        self.old_task_new_task_map = (
            self.gsheet_processor.get_old_task_new_task_mapping()
        )

        self.task_id_mapping = {}
        self.operator_params = {}

        self.workflow_config = self._read_airflow_content()

    def _read_airflow_content(self):
        folder_path = f"{self.workflow_setting_path}/{self.workflow_name}"

        if not os.path.exists(folder_path):
            raise FileNotFoundError(f"Folder not found: {folder_path}")

        for file_name in os.listdir(folder_path):
            if file_name == "workflow_setting.py":
                # Construct the full path to the content file
                file_path = os.path.join(folder_path, file_name)

                # Read and return the content file
                with open(file_path, "r") as file:
                    file_content = file.read()

                return file_content

        raise FileNotFoundError(
            f"No config file found for workflow: {self.workflow_name}"
        )

    def extract_task_mappings_and_params(self, file_content):
        operator_pattern = r"(\w+)\s*=\s*[\w\.]+(?:Operator|Sensor)\((.*?)\)"
        operators = re.findall(operator_pattern, file_content, re.DOTALL)
        for operator in operators:
            task_key, parameters = operator

            task_id_match = re.search(r"task_id=['\"]([\w\.]+)['\"]", parameters)
            if not task_id_match:
                print(f"Warning: No task_id found for operator {task_key}")
                continue

            task_id = self._convert_old_task_key_to_new(task_id_match.group(1)).replace(
                ".", "-"
            )
            self.task_id_mapping[task_key] = task_id

            params_match = re.search(r"(?:params|parameters)=\{(.*?)\}", parameters)
            if params_match:
                self.operator_params[task_id] = self.reset_values_to_empty(
                    "{" + params_match.group(1) + "}"
                )
            else:
                print(
                    f"Info: No params found for task_id {task_id} of operator {task_key}"
                )

    def reset_values_to_empty(self, input_string):
        key_pattern = r"(['\"]?\w+['\"]?)\s*:"
        keys = re.findall(key_pattern, input_string)

        params_dict = {}
        for key in keys:
            params_dict[key.strip("'\"")] = ""

        return params_dict

    def _convert_old_task_key_to_new(self, old_task_key):
        if old_task_key in self.old_task_new_task_map:
            return self.old_task_new_task_map[old_task_key]
        else:
            warnings.warn(
                f"Old task key '{old_task_key}' not found in GSheet, old task name or code will be used.",
                UserWarning,
            )
            return old_task_key

    def preprocess_content(self, file_content):
        cleaned_content = re.sub(r"\[\s*", "[", file_content)
        cleaned_content = re.sub(r",\s*\n\s*", ", ", cleaned_content)
        return cleaned_content

    def parse_dependencies(self, dependency_str):
        dependencies = {}
        lines = dependency_str.strip().split("\n")
        for line in lines:
            line = re.sub(r"\s+", " ", line)
            line = re.sub(r"\[ ", "[", line)
            if ">>" in line:
                tasks = re.split(r"\s*>>\s*", line)
                tasks = [re.sub(r"[ \[\]]+", "", task).split(",") for task in tasks]
                for i in range(1, len(tasks)):
                    for task in tasks[i]:
                        normalized_task = self._convert_old_task_key_to_new(
                            task
                        ).replace(".", "-")
                        if normalized_task and normalized_task not in dependencies:
                            dependencies[normalized_task] = {
                                "task_key": normalized_task,
                                "depends_on": [],
                                "notebook_task": self.operator_params.get(
                                    normalized_task, {}
                                ),
                            }
                        dependencies[normalized_task]["depends_on"].extend(
                            [
                                {
                                    "task_key": self._convert_old_task_key_to_new(
                                        dep
                                    ).replace(".", "-")
                                }
                                for dep in tasks[i - 1]
                                if dep
                            ]
                        )
            else:
                # More specific pattern for standalone task assignments
                standalone_task_match = re.search(
                    r"(\w+)\s*=\s*[\w\.]+Operator\(", line
                )
                if standalone_task_match:
                    task = standalone_task_match.group(1)
                    normalized_task = self._convert_old_task_key_to_new(task).replace(
                        ".", "-"
                    )
                    if normalized_task and normalized_task not in dependencies:
                        dependencies[normalized_task] = {
                            "task_key": normalized_task,
                            "depends_on": [],
                            "notebook_task": self.operator_params.get(
                                normalized_task, {}
                            ),
                        }
        return list(dependencies.values())

    def extract_dependencies(self):
        file_content = self._read_airflow_content()
        self.extract_task_mappings_and_params(file_content)
        cleaned_content = self.preprocess_content(file_content)
        dependencies = self.parse_dependencies(cleaned_content)
        return dependencies

    def build_target_tasks(self):
        dependencies = self.extract_dependencies()
        path_map = self.gsheet_processor.get_new_task_path_mapping()
        for dep in dependencies:
            task_key = dep["task_key"]
            dep["notebook_task"] = {
                "notebook_path": os.path.join(REPOS_PREFIX, path_map.get(task_key, "")),
                "source": "WORKSPACE",
                "base_parameters": self.operator_params.get(task_key, {}),
            }
        return {"tasks": dependencies}

    def build_save_target_tasks(self):
        target_tasks = self.build_target_tasks()
        output_path = os.path.join(
            TARGET_TASKS_PATH, f"{self.workflow_name}_tasks_config.json"
        )
        print(f"Writing {output_path}")
        with open(output_path, "w") as f:
            json.dump(target_tasks, f)
        f.close()
        return target_tasks
