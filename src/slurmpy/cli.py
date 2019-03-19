import os
import os.path as osp
from copy import copy

import click
import toml

import slurmpy.slurmpy as slm

RUN_DEFAULTS = {
    'nodes' : 1,
    'ntasks' : 1,
    'mail_type' : slm.MAIL_TYPE_DEFAULT,
    'epilog' : None,
}


CLI_TO_TEMPLATE_KEYS = (
    ('num_cpus', 'cpus_per_task'),
    ('memory', 'node_mem'),
    ('email', 'mail_user'),
)

INT_TO_STR = ('num_cpus', 'num_gpus',)

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

def _normalize_context_kwargs(context_kwargs):

    if 'setup' in context_kwargs['context']:

        setup_kwargs = context_kwargs['context'].pop('setup')

        # convert the environment variables to tuples
        setup_kwargs['env_vars'] = [(key, value) for key, value in
                                     setup_kwargs['env_vars'].items()]
    else:
        setup_kwargs = {}

    if 'teardown' in context_kwargs['context']:
        teardown_kwargs = context_kwargs['context'].pop('teardown')
    else:
        teardown_kwargs = {}

    return context_kwargs['context'], setup_kwargs, teardown_kwargs

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


    # then cast certain values to strings
    for cli_key, value in run_kwargs.items():
        if cli_key in INT_TO_STR:
            run_kwargs[cli_key] = str(value)

    # then just rename the other ones to the template key
    for cli_key, template_key in CLI_TO_TEMPLATE_KEYS:

        if cli_key in run_kwargs:
            run_kwargs[template_key] = run_kwargs.pop(cli_key)

    return run_kwargs

@click.command()
@click.option('--config', type=click.Path(exists=True), default=None,
              help="""Configuration file specifying job submission (and context if given) values.
                    All other options which are given will override the config values.""")
@click.option('--epilog', type=click.Path(exists=True), default=None,
              help="""Script to be run in all cases (even in failure) after the command""")
@click.option('--constraint', default=None)
@click.option('--walltime', default=None)
@click.option('--memory', default=None)
@click.option('--num_cpus', default=None)
@click.option('--num_gpus', default=None)
@click.option('--context', type=click.Path(exists=True), default=None,
              help="""Configuration file specifying context setup and teardown values.
                    Options which are given will override the config values,
              but this overrides options in the 'config' option.""")
@click.option('--command', default=None)
@click.option('--embed-script', default=False,
              help="Will embed scripts to the slurmified script (no srun formatting is done).")
@click.option('--script-in', type=click.Path(exists=True, dir_okay=False, resolve_path=True),
              default=None)
@click.option('--script-out', type=click.Path(exists=False, dir_okay=False, resolve_path=True),
              default=None)
@click.option('--batch-in', type=click.Path(exists=True, file_okay=False, resolve_path=True),
              default=None)
@click.option('--batch-out', type=click.Path(exists=False, file_okay=False, resolve_path=True),
              default=None)
@click.argument('job_name')
def slurmify(config, epilog, constraint, walltime, memory, num_cpus, num_gpus,
             context,
             command, embed_script, script_in, script_out, batch_in, batch_out,
             job_name):

    # expand all the paths to the full paths
    if config is not None:
        config = osp.abspath(osp.expanduser(config))
    if script_in is not None:
        script_in = osp.abspath(osp.expanduser(script_in))
    if script_out is not None:
        script_out = osp.abspath(osp.expanduser(script_out))
    if batch_in is not None:
        batch_in = osp.abspath(osp.expanduser(batch_in))
    if batch_out is not None:
        batch_out = osp.abspath(osp.expanduser(batch_out))

    # check to make sure we have inputs to slurmify

    # if we have ins and outs for the batch we need both in and out
    # targets
    batch_ok = False
    if (batch_in is not None) and (batch_out is not None):
        batch_ok = True

    # can do one of: command, script, or batch
    options_selected = (True if command is not None else False,
                        True if script_in is not None else False,
                        True if batch_ok else False)

    num_selected = len([() for option
                        in options_selected
                        if option])
    if num_selected > 1:
        raise ValueError("Choose only one input option: command, script, or batch")

    # if only one is chosen then figure which one
    option_idxs = ('command', 'script', 'batch')
    option_selected = option_idxs[[i for i, sel in enumerate(options_selected)
                                   if sel][0]]

    # gather the options that can override the config file
    option_kvs = (
        ('constraint', constraint),
        ('walltime', walltime),
        ('memory', memory),
        ('num_cpus', num_cpus),
        ('num_gpus', num_gpus),
    )

    # load the run settings config file
    if config is not None:
        run_kwargs = toml.load(config)
    else:
        run_kwargs = {}


    # update values from the options if given
    for opt_key, option in option_kvs:

        if option is not None:
            run_kwargs[opt_key] = option

    # apply the defaults for these options for those that are
    # hardcoded here or not given
    run_kwargs = _apply_run_defaults(run_kwargs)

    # normalize the keys to the keys they correspond to in the actual
    # template, translating them from this input interface to that one
    run_kwargs = _normalize_template_kwargs(run_kwargs)

    # load the run settings config file
    if context is not None:
        context_kwargs = toml.load(context)
    else:
        context_kwargs = {'context' : {}}

    # override the values in from the base config file
    context_kwargs, setup_kwargs, teardown_kwargs = _normalize_context_kwargs(context_kwargs)

    # get the job header for this job
    sjob = slm.SlurmJob(job_name)
    job_header = sjob.job_header

    # the environment with the templates
    env = slm.get_env()

    # get the templates for the run header and the script template
    run_template = env.get_template(slm.SLURM_RUN_TEMPLATE)
    commands_template = env.get_template(slm.SLURM_COMMANDS_TEMPLATE)
    script_template = env.get_template(slm.SLURM_SCRIPT_TEMPLATE)
    setup_template = env.get_template(slm.SLURM_SETUP_TEMPLATE)
    teardown_template = env.get_template(slm.SLURM_TEARDOWN_TEMPLATE)

    # render the run options header
    run_header = run_template.render(**run_kwargs)

    # generate the payloads depending on the type of input: command,
    # script, batch

    # this is used for batch
    in_names = []

    if option_selected == 'command':
        # render the commands for the payload
        payloads = [commands_template.render(commands=[command],
                                             epilog=context_kwargs['epilog'])]

    elif option_selected == 'script':

        # read the script
        with open(script_in, 'r') as rf:
            script = rf.read()

        # and set it as the payload without adding sruns like we do
        # for command

            # embed the script into the script as the payload, if this
            # was requested
            if embed_script:

                with open(script_in, 'r') as rf:
                    script = rf.read()

                payloads = [script]

                setups = [setup_template.render(**setup_kwargs, **context_kwargs)]
                teardowns = [teardown_template.render(**teardown_kwargs, **context_kwargs)]


            # otherwise just treat it like a command
            else:

                payloads = [commands_template.render(commands=[script_in],
                                                     epilog=context_kwargs['epilog'])]

                # since we are not embedding it we want to copy that
                # script to the execution environmen at runtime, so we
                # modify the context scripts to deal with this by
                # modifying kwargs for them
                setups = [setup_template.render(task_script=script_in,
                                               **setup_kwargs, **context_kwargs)]
                teardowns = [teardown_template.render(task_script=script_in,
                                                     **teardown_kwargs, **context_kwargs)]




    elif option_selected == 'batch':

        # just generate a script payload for everything in the batch
        # dir
        payloads = []
        setups = []
        teardowns = []
        for script_file in os.listdir(batch_in):

            in_path = osp.join(batch_in, script_file)

            # embed the script into the script as the payload, if this
            # was requested
            if embed_script:

                with open(in_path, 'r') as rf:
                    script = rf.read()

                payload = script
                setup = setup_template.render(**setup_kwargs, **context_kwargs)
                teardown = teardown_template.render(**teardown_kwargs, **context_kwargs)


            # otherwise just treat it like a command
            else:
                payload = commands_template.render(commands=[in_path],
                                                   epilog=context_kwargs['epilog'])

                # since we are not embedding it we want to copy that
                # script to the execution environmen at runtime, so we
                # modify the context scripts to deal with this by
                # modifying kwargs for them
                setup = setup_template.render(task_script=in_path,
                                              **setup_kwargs, **context_kwargs)
                teardown = teardown_template.render(task_script=in_path,
                                                    **teardown_kwargs, **context_kwargs)

            payloads.append(payload)
            setups.append(setup)
            teardowns.append(teardown)
            in_names.append(script_file)




    scripts = []
    for i, payload in enumerate(payloads):

        setup = setups[i]
        teardown = teardowns[i]

        # get the script kwargs
        script_kwargs = {'slurm_job' : job_header,
                         'slurm_run' : run_header,
                         'setup' : setup,
                         'payload' : payload,
                         'teardown' : teardown}

        # render the script
        script = script_template.render(**script_kwargs)

        scripts.append(script)

    # choose how to output the files
    if option_selected == 'batch' and batch_out:

        # first make sure the batch_out dir is there
        os.makedirs(batch_out, exist_ok=True)

        for i, script in enumerate(scripts):
            # get the path to save the script
            out_name = "{}.slurm".format(in_names[i])
            out_path = osp.join(batch_out, out_name)

            # write it
            with open(out_path, 'w') as wf:
                wf.write(script)

    # we write out the script from either command or script in
    elif script_out is not None:
        with open(script_out, 'w') as wf:
            wf.write(scripts[0])

    # otherwise just send it out
    else:
        click.echo(scripts[0])

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
