import os.path as osp
from copy import copy

import click
import toml

import slurmpy.slurmpy as slm

RUN_DEFAULTS = {
    'nodes' : 1,
    'ntasks' : 1,
    'mail_type' : slm.MAIL_TYPE_DEFAULT
}


CLI_TO_TEMPLATE_KEYS = (
    ('num_cpus', 'cpus_per_task'),
    ('memory', 'node_memory'),
    ('email', 'mail_user'),
)

CLI_TO_GRE_KEYS = (
    ('num_gpus', 'gpu'),
)

GRE_TEMPLATES = (
    ('gpu', "gpu:{num}"),
)

GRE_JOIN_CHAR = ','

def _apply_run_defaults(run_kwargs, run_defaults=RUN_DEFAULTS):

    for key, value in RUN_DEFAULTS.items():
        if key not in run_kwargs:
            run_kwargs[key] = value

    return run_kwargs

def _normalize_template_kwargs(run_kwargs):

    run_kwargs = copy(run_kwargs)

    gre_templates = dict(GRE_TEMPLATES)

    # first need to generate the GREs string from any matching GRE key
    substrings = []
    for cli_key, gre_key in CLI_TO_GRE_KEYS:

        if cli_key in run_kwargs:
            # fill out the template for that resource, popping the
            # value out from the CLI key
            resource_substring = gre_templates[gre_key].format(num=run_kwargs.pop(cli_key))

            substrings.append(resource_substring)

    run_kwargs['gres'] = GRE_JOIN_CHAR.join(substrings)

    # then just rename the other ones to the template key
    for cli_key, template_key in CLI_TO_TEMPLATE_KEYS:

        if cli_key in run_kwargs:
            run_kwargs[template_key] = run_kwargs.pop(cli_key)

    return run_kwargs

@click.command()
@click.option('--config', type=click.Path(exists=True), default=None,
              help="""Configuration file specifying all optional values.
                    Options which are given will override the config values.""")
@click.option('--constraint', default=None)
@click.option('--walltime', default=None)
@click.option('--memory', default=None)
@click.option('--num_cpus', default=None)
@click.option('--num_gpus', default=None)
@click.argument('job_name')
@click.argument('command')
def slurmify(config, constraint, walltime, memory, num_cpus, num_gpus, job_name, command):

    # load the config file
    if config is not None:
        run_kwargs = toml.load(config)
    else:
        run_kwargs = {}

    option_kvs = (
        ('constraint', constraint),
        ('walltime', walltime),
        ('memory', memory),
        ('num_cpus', num_cpus),
        ('num_gpus', num_gpus)
    )


    # update values from the options if given
    for opt_key, option in option_kvs:

        if option is not None:
            run_kwargs[opt_key] = option

    print(run_kwargs)

    run_kwargs = _apply_run_defaults(run_kwargs)

    # normalize the keys to the keys they correspond to in the actual
    # template, translating them from this input interface to that one
    run_kwargs = _normalize_template_kwargs(run_kwargs)


    # get the job header for this job
    sjob = slm.SlurmJob(job_name)
    job_header = sjob.job_header
    setup = sjob.setup
    teardown = sjob.teardown

    # the environment with the templates
    env = slm.get_env()

    # get the templates for the run header and the script template
    run_template = env.get_template(slm.SLURM_RUN_TEMPLATE)
    commands_template = env.get_template(slm.SLURM_COMMANDS_TEMPLATE)
    script_template = env.get_template(slm.SLURM_SCRIPT_TEMPLATE)

    # render the run options header
    run_header = run_template.render(**run_kwargs)

    # render the commands for the payload
    payload = commands_template.render(commands=[command])

    # get the script kwargs
    script_kwargs = {'slurm_job' : job_header,
                     'slurm_run' : run_header,
                     'setup' : setup,
                     'payload' : payload,
                     'teardown' : teardown}

    # render the script
    script_str = script_template.render(**script_kwargs)

    click.echo(script_str)

@click.command()
@click.argument('walltime')
@click.argument('memory')
@click.argument('num_cpus')
@click.argument('job_name')
@click.argument('command')
def command(walltime, memory, num_cpus, job_name, command):
    """Run a single command on a single node."""

    sjob = slm.SlurmJob(job_name)

    run_kwargs = {'walltime' : walltime,
                  'memory' : memory,
                  'num_cpus' : num_cpus,}

    run_kwargs = _apply_run_defaults(run_kwargs)

    jobid, completed_process = sjob.run(run_kwargs, [command])

    click.echo("JOBID: {}".format(jobid))

    # click.echo(click.style("JOBID: {}", fg='green').format(
    #     click.style(jobid, fg='red', bold=True)))

    click.echo(completed_process.stdout)
    click.echo(completed_process.stderr)

@click.group()
def cli():
    """ """
    pass


cli.add_command(command)
cli.add_command(slurmify)

if __name__ == "__main__":

    cli()
