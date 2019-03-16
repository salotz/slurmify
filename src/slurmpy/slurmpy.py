import os
import os.path as osp
import tempfile
import subprocess

from jinja2 import Environment, FileSystemLoader

from slurmpy import TEMPLATES_PATH

# names of the templates
SLURM_JOB_TEMPLATE = "slurm_job.sh.j2"
SLURM_RUN_TEMPLATE = "slurm_run.sh.j2"
SLURM_SCRIPT_TEMPLATE = "slurm_script.sh.j2"
SLURM_COMMANDS_TEMPLATE = "slurm_commands.sh.j2"

# list of all the templates
SLURM_TEMPLATE_NAMES = (SLURM_JOB_TEMPLATE, SLURM_RUN_TEMPLATE,
                        SLURM_SCRIPT_TEMPLATE, SLURM_COMMANDS_TEMPLATE)

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
    ('cpus_per_task', False),
    ('mem_per_cpu', True),
    ('node_mem', True),
    ('nodelist', True),
    ('constraint', True),
    ('gres', True),
    ('chdir', True),
)

SLURM_SCRIPT_TARGETS = (
    ('slurm_job', False),
    ('slurm_run', False),
    ('setup', True),
    ('payload', False),
    ('teardown', True),
)

SLURM_COMMANDS_TARGETS = (
    ('commands', False)
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
            self._job_header = job_template.render(self.job_kwargs)
        else:
            raise ValueError

        # get default setup and teardown scripts
        self._setup = ""
        self._teardown = ""

    @property
    def job_header(self):
        return self._job_header

    @property
    def setup(self):
        return self._setup

    @property
    def teardown(self):
        return self._teardown

    def run(self, run_kwargs, commands):

        run_template = self.env.get_template(SLURM_RUN_TEMPLATE)
        commands_template = self.env.get_template(SLURM_COMMANDS_TEMPLATE)
        script_template = self.env.get_template(SLURM_SCRIPT_TEMPLATE)

        if len(check_kwargs(SLURM_RUN_TARGETS, run_kwargs)) < 1:
            run_header = run_template.render(run_kwargs)
        else:
            raise ValueError

        payload = commands_template.render(commands=commands)

        script_kwargs = {
            'slurm_job' : self.job_header,
            'slurm_run' : run_header,
            'setup' : self.setup,
            'payload' : payload,
            'teardown' : self.teardown
        }

        if len(check_kwargs(SLURM_SCRIPT_TARGETS, script_kwargs)) < 1:
            script_str = script_template.render(script_kwargs)
        else:
            raise ValueError

        # make a temporary file for the script and use sbatch to
        # submit it

        with tempfile.NamedTemporaryFile() as tmpfile:

            # write the script to the tempfile
            tmpfile.write(str.encode(script_str))

            # set the file pointer back to the beginning of the file
            # so we don't have to reopen it
            tmpfile.seek(0)

            # the path to the temp file
            tmpfile_path = tmpfile.name

            # then actually submit the script and get the return
            # values
            complete_process = subprocess.run(['sbatch', tmpfile_path])

            # if there was error get it:
            if complete_process.stderr is not None:
                pass

            # get the jobid from stdout
            #complete_process.stdout

        return 0, complete_process
