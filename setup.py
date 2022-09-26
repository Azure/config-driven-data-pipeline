import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="simple-cddp",
    version="0.0.1",
    author="Sean Ma",
    author_email="maye-msft@outlook.com",
    description="Cogfig Driven Data Pipeline",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/maye-msft/simple-configurable-data-pipeline",
    project_urls={
        "Bug Tracker": "https://github.com/maye-msft/simple-configurable-data-pipeline/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.8",
)