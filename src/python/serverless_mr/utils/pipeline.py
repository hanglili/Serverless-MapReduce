class Pipeline:
    def __init__(self):
        self.config = {}
        self.functions = []
        self.dependent_pipelines_ids = []

    def get_config(self):
        return self.config

    def get_functions(self):
        return self.functions

    def get_dependent_pipeline_ids(self):
        return self.dependent_pipelines_ids

    def add_function(self, function):
        self.functions.append(function)

    def set_config(self, config):
        self.config = config

    def set_dependent_pipelines_ids(self, dependent_pipeline_ids):
        self.dependent_pipelines_ids = dependent_pipeline_ids
