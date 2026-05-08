from importlib.metadata import PackageNotFoundError, version


try:
    __version__ = version("sisap-light-pipeline")
except PackageNotFoundError:
    __version__ = "0.0.0"
