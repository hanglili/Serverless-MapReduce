import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="serverless-mr", # Replace with your own username
    version="4.1.0",
    author="Hang Li Li",
    author_email="hl4716@ic.ac.uk",
    description="Serverless Map Reduce",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/hanglili/Serverless-MapReduce",
    # package_dir={'': 'serverless_mr'},
    # packages=setuptools.find_packages('serverless_mr',
    #                                   exclude=["js", "node_modules", "package-lock.json"]
    # ),
    packages=setuptools.find_packages(
        exclude=["*.tests", "*.tests.*", "tests.*", "tests", "js", "node_modules", "user_job*", "package-lock.json",
                 ".serverless", "cloudpickle", "configuration", "pipeline", "user_main.*", "test_trigger.*"]
    ),
    package_data={
        '': ['templates/static/*.html', 'templates/public/*.*'],
    },
    data_files=[('.', ['requirements.txt', 'package.json', 'serverless.yml'])],
    # include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)