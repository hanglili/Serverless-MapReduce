from static.static_variables import StaticVariables


class ReduceFunction:

    def __init__(self, reduce_function, rel_function_path, num_reducers):
        self.reduce_function = reduce_function
        self.rel_function_path = rel_function_path
        self.num_reducers = num_reducers

    def get_function(self):
        return self.reduce_function

    def get_rel_function_path(self):
        return self.rel_function_path

    def get_num_reducers(self):
        return self.num_reducers

    def get_string(self):
        return "reduce"

    def get_handler_function_path(self):
        return StaticVariables.REDUCE_HANDLER_FUNCTION_PATH

    def get_rel_function_paths(self):
        return [self.rel_function_path]
