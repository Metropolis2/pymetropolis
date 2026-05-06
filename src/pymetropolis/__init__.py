from importlib.metadata import version

from .cli import app as app
from .main import main as main

__version__ = version("pymetropolis")
