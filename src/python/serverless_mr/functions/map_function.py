from serverless_mr.static.static_variables import StaticVariables


class MapFunction:

    def __init__(self, map_function, rel_function_path):
        self.map_function = map_function
        self.rel_function_path = rel_function_path

    def get_function(self):
        return self.map_function

    def get_rel_function_path(self):
        return self.rel_function_path

    def get_string(self):
        return "map"

    def get_handler_function_path(self):
        return StaticVariables.MAP_HANDLER_FUNCTION_PATH

    def get_rel_function_paths(self):
        return [self.rel_function_path]
