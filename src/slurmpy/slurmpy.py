import pkg_resources
import os
import os.path as osp

from jinja2 import Environment, FileSystemLoader

from slurmpy import TEMPLATES_PATH



# names of the templates
SLURM_JOB_TEMPLATE = "slurm_job.sh.j2"
SLURM_RUN_TEMPLATE = "slurm_job.sh.j2"
SLURM_SCRIPT_TEMPLATE = "slurm_job.sh.j2"

# list of all the templates
SLURM_TEMPLATE_NAMES = (SLURM_JOB_TEMPLATE, SLURM_RUN_TEMPLATE,
                        SLURM_SCRIPT_TEMPLATE)

# the names of the targets and whether or not they are optional (True)
# or not (False). This is from the perspective of the template and not
# the interfaces of the programs which may support defaults and other
# options etc.
SLURM_JOB_TARGETS = (
    ('job_name', False),
    ('stderr_log_dir', False),
    ('stdout_log_dir', False),
    ('login_shell', True),
    ('mail_user', True),
    ('mail_type', True),
)

SLURM_RUN_TARGETS = (
    ('walltime', False),
    ('nodes', False),
    ('ntasks', False),
    ('cpus-per-task', False),
    ('mem_per_cpu', True),
    ('node_mem', True),
    ('nodelist', True),
    ('constraint', True),
    ('gres', True),
    ('chdir', True),
)

SLURM_SCRIPT_TARGETS = (
    ('slurm_job', True),
    ('slurm_run', True),
    ('setup', True),
    ('payload', True),
    ('teardown', True),
)

# Defaults
MAIL_TYPE_DEFAULT = "BEGIN,END,FAIL"

def get_env():
    return Environment(loader=FileSystemLoader(TEMPLATES_PATH))

def check_kwargs(targets, input_kwargs):

    missing = []
    for key, optional in targets:
        if key not in input_kwargs:
            if not optional:
                missing.append(key)

    return missing

class SlurmJob():

    def __init__(self, job_name,
                 logs_dir='logs',
                 setup=None,
                 teardown=None,
                 login_shell=True,
                 email=None,
                 mail_type='BEGIN,END,FAIL'):

        if logs_dir:
            stderr_log_dir = logs_dir
            stdout_log_dir = logs_dir

        self.job_kwargs = {
            'job_name' : job_name,
            'stderr_log_dir' : stderr_log_dir,
            'stdout_log_dir' : stdout_log_dir,
            'login_shell' : login_shell,
            'mail_user' : email,
            'mail_type' : mail_type
        }

        self.env = get_env()

        job_template = self.env.get_template(SLURM_JOB_TEMPLATE)

        # check to make sure all arguments work
        if len(check_kwargs(SLURM_JOB_TARGETS, self.job_kwargs)) < 1:
            self.job_header = job_template.render(self.job_kwargs)
        else:
            raise ValueError

        # get default setup and teardown scripts
        self.setup = ""
        self.teardown = ""

    def run(run_kwargs, commands):

        run_template = self.env.get_template(SLURM_RUN_TEMPLATE)
        script_template = self.env.get_template(SLURM_SCRIPT_TEMPLATE)

        if len(check_kwargs(SLURM_RUN_TARGETS, run_kwargs)) < 1:
            run_header = run_template.render(run_kwargs)
        else:
            raise ValueError


        script_kwargs = {
            'slurm_job' : self.job_header,
            'slurm_run' : run_header,
            'setup' : self.setup,
            'commands' : commands,
            'teardown' : self.teardown
        }

        if len(check_kwargs(SLURM_SCRIPT_TARGETS, script_kwargs)) < 1:
            script = run_template.render(script_kwargs)
        else:
            raise ValueError


        # then actually submit the script
        subprocess.run(['sbatch', ], )
