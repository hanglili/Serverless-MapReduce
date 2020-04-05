import io
import pickle


class RenameUnpickler(pickle.Unpickler):
    def find_class(self, module, name):
        renamed_module = module
        if module == "pipeline.pipeline_test":
            renamed_module = "pipeline_test"

        return super(RenameUnpickler, self).find_class(renamed_module, name)


def renamed_load(file_obj):
    return RenameUnpickler(file_obj).load()


def renamed_loads(pickled_bytes):
    file_obj = io.BytesIO(pickled_bytes)
    return renamed_load(file_obj)

outputs = []
with open('src/python/pipeline/foo.pkl', 'rb') as f:
    map_function_ = renamed_load(f)

print("Hello")
map_function_(outputs, [1, '127.0.0.1, dasda, dasda, 1.0, dasdsa'])
print(outputs)