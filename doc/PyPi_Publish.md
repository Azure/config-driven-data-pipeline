# Publish PyPi

## Install Dependencies

To build and upload your package to PyPI, you’ll use two tools called Build and Twine. You can install them using pip as usual:

```bash
python -m pip install build twine
```

## Build Your Package

Running the following command from the same directory as the `pyproject.toml` file, where check the version is correct:

```bash
python -m build
```

## Confirm Your Package Build

```bash
twine check dist/*
```

## Upload Your Package

```bash
twine upload dist/*
```
