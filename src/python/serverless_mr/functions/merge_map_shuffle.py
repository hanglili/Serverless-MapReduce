from serverless_mr.static.static_variables import StaticVariables


class MergeMapShuffleFunction:

    def __init__(self, map_function, rel_map_function_path, partition_function, rel_partition_function_path):
        self.map_function = map_function
        self.partition_function = partition_function
        self.rel_map_function_path = rel_map_function_path
        self.rel_partition_function_path = rel_partition_function_path

    def get_function(self):
        return self.map_function

    def get_rel_function_path(self):
        return self.rel_map_function_path

    def get_partition_function(self):
        return self.partition_function

    def get_rel_partition_function_path(self):
        return self.rel_partition_function_path

    def get_string(self):
        return "merge-map-shuffle"

    def get_handler_function_path(self):
        return StaticVariables.MAP_SHUFFLE_HANDLER_FUNCTION_PATH

    def get_rel_function_paths(self):
        return [self.rel_map_function_path, self.rel_partition_function_path]
