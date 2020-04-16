import sys
from modulefinder import ModuleFinder
finder = ModuleFinder()
finder.run_script("../serverless_mr/driver/driver.py")
for name, mod in finder.modules.items():
    if not name.startswith('__'):
        print(name)

# modulenames = set(sys.modules) & set(globals())
# allmodules = [sys.modules[name] for name in modulenames]
#
# print [key for key in locals().keys()
#        if isinstance(locals()[key], type(sys)) and not key.startswith('__')]