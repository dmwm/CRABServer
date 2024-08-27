#! /usr/bin/env python
"""
The python helper to parse args (use pure bash is a nightmare).
We can even convert shell script in to python to increase code maintainability.
"""
import argparse
import os


class EnvDefault(argparse.Action):
    """
    # copy from https://stackoverflow.com/a/10551190
    # to be able to read from env if args not provided.
    """
    def __init__(self, envvar, required=True, default=None, **kwargs):
        if envvar:
            if envvar in os.environ:
                default = os.environ[envvar]
        if required and default:
            required = False
        super().__init__(default=default, required=required, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)

parser = argparse.ArgumentParser(description='crab service process controller')

subparsers = parser.add_subparsers(dest='command', required=True, help='command to run')
parserStart = subparsers.add_parser('start',
                                    help='start the service')
groupModeStart = parserStart.add_mutually_exclusive_group(required=True)
groupModeStart.add_argument('-c', dest='mode', action='store_const', const='current',
                            help='start service from installed directory')
groupModeStart.add_argument('-g', dest='mode', action='store_const', const='fromGH',
                            help='start service from /data/repos directory')
parserStart.add_argument('-d', dest='debug', action='store_const', const='t',
                         default='f',
                         help='Enable debug mode (foreground)')
# env $SERVICE
parserStart.add_argument('-s', dest='service', action=EnvDefault, envvar='SERVICE',
                         help='Name of the service to run: REST/TaskWorker*/Publisher*')
parserStop = subparsers.add_parser('stop',
                                    help='show service status (exit non-zero if service does not start)')
parserStatus = subparsers.add_parser('status',
                                    help='start the service')
parserEnv = subparsers.add_parser('env',
                                  help='print environment variable for sourcing it locally.')
groupModeEnv = parserEnv.add_mutually_exclusive_group(required=True)
groupModeEnv.add_argument('-c', dest='mode', action='store_const', const='current',
                            help='export PYTHONPATH from installed directory')
groupModeEnv.add_argument('-g', dest='mode', action='store_const', const='fromGH',
                            help='export PYTHONPATH from /data/repos directory')

args = parser.parse_args()

env = os.environ.copy()
# always provides env vars
env['COMMAND'] = args.command
env['MODE'] = getattr(args, 'mode', 'current')
env['DEBUG'] = getattr(args, 'debug', '')
env['SERVICE'] = getattr(args, 'service', '')

# re exec the ./manage.sh
# os.execle(filepath, arg0, arg1, ..., argN, env_dict)
os.execle('./manage.sh','./manage.sh', env)
