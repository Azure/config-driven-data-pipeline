import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="cddp",
    version="0.1.9",
    author="Sean Ma",
    author_email="maye-msft@outlook.com",
    description="Config Driven Data Pipeline",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/azure/config-driven-data-pipeline",
    project_urls={
        "Bug Tracker": "https://github.com/azure/config-driven-data-pipeline/issues",
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
