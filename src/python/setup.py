import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="serverless-mr", # Replace with your own username
    version="1.4.0",
    author="Hang Li Li",
    author_email="hl4716@ic.ac.uk",
    description="Serverless Map Reduce",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/hanglili/Serverless-MapReduce",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)