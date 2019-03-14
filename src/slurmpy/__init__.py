import pkg_resources
import os
import os.path as osp

TEMPLATES_PATH = pkg_resources.resource_filename('slurmpy', 'templates')

TEMPLATE_FILENAMES = tuple(os.listdir(TEMPLATES_PATH))
TEMPLATE_FILEPATHS = tuple([osp.join(TEMPLATES_PATH, filename) for filename
                            in TEMPLATE_FILENAMES])
