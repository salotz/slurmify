import pkg_resources
import os
import os.path as osp

from jinja2 import Environment, FileSystemLoader

from slurmpy import TEMPLATES_PATH

def get_env():
    return Environment(loader=FileSystemLoader(TEMPLATES_PATH))
