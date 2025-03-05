import pandas as pd


class GsheetProcessor:
    def __init__(self, workflow_name, gsheet_task_list_all_path):
        self.gsheet_task_list_all_path = gsheet_task_list_all_path
        self.workflow_name = workflow_name

        self.df = self._read_gsheet_task_list(self.gsheet_task_list_all_path)
        self.df = self.df[self.df["CURRENT_WORKFLOW_NAME"] == self.workflow_name]

    def _read_gsheet_task_list(self, file_path):
        return pd.read_csv(file_path)

    def get_old_task_name_list(self):
        return_list = list(self.df["OLD_TASK_NAME"])
        return return_list

    def get_new_task_name_list(self):
        return_list = list(self.df["NEW_TASK_NAME"])
        return return_list

    def get_git_path_list(self):
        return_list = list(self.df["NEW_GIT_PATH"])
        return return_list

    def get_new_workflow_name(self):
        return list(self.df["DATABRICKS_WORKFLOW_NAME"])[0]

    def get_old_task_new_task_mapping(self):
        mapping = {}
        old_task_list = self.get_old_task_name_list()
        new_task_list = self.get_new_task_name_list()

        for old_task, new_task in zip(old_task_list, new_task_list):
            mapping[old_task] = new_task

        return mapping

    def get_new_task_path_mapping(self):
        mapping = {}
        new_task_list = self.get_new_task_name_list()
        git_path_list = self.get_git_path_list()

        for new_task, git_path in zip(new_task_list, git_path_list):
            mapping[new_task] = git_path.replace(".sql", "")

        return mapping
