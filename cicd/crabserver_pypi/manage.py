#!/usr/bin/env python
"""
The python helper to parse args (use pure bash is a nightmare).
We can even convert shell script in to python to increase code maintainability.
"""
import argparse
import os

MANAGE_SH_PATH = os.path.join(__file__, 'manage.sh')

class EnvDefault(argparse.Action): # pylint: disable=too-few-public-methods
    """
    # copy from https://stackoverflow.com/a/10551190
    # to make arg able to read from env if not provided.
    """
    def __init__(self, envvar, required=False, default=None, **kwargs):
        if envvar:
            if envvar in os.environ:
                default = os.environ[envvar]
        if required and default:
            required = False
        super().__init__(default=default, required=required, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)

myparser = argparse.ArgumentParser(description='crab service process controller')

subparsers = myparser.add_subparsers(dest='command', required=True, help='command to run')
parserStart = subparsers.add_parser('start',
                                    help='start the service')
groupModeStart = parserStart.add_mutually_exclusive_group(required=True)
groupModeStart.add_argument('-c', dest='mode', action='store_const', const='current',
                            help='start service from installed directory')
groupModeStart.add_argument('-g', dest='mode', action='store_const', const='fromGH',
                            help='start service from /data/repos directory')
parserStart.add_argument('-d', dest='debug', action='store_const', const='t',
                         default='',
                         help='Enable debug mode (foreground)')
# env $SERVICE
parserStart.add_argument('-s', dest='service', action=EnvDefault, envvar='SERVICE',
                         default='',
                         help='Name of the service to run. Only use in Publisher (Publisher_schedd, Publisher_rucio)')
parserStop = subparsers.add_parser('stop',
                                   help='show service status (exit non-zero if service does not start)')
parserStatus = subparsers.add_parser('status',
                                    help='start the service')
parserEnv = subparsers.add_parser('env',
                                  help='print environment variable for sourcing it locally.')
# Wa: it actually the same as groupModeStart but I do not know how to reuse it
# in another parser.
groupModeEnv = parserEnv.add_mutually_exclusive_group(required=True)
groupModeEnv.add_argument('-c', dest='mode', action='store_const', const='current',
                            help='export PYTHONPATH from installed directory')
groupModeEnv.add_argument('-g', dest='mode', action='store_const', const='fromGH',
                            help='export PYTHONPATH from /data/repos directory')

args = myparser.parse_args()

env = os.environ.copy()
# always provides env vars
env['COMMAND'] = args.command
env['MODE'] = args.mode if hasattr(args, 'mode') else ''
env['DEBUG'] = args.debug if hasattr(args, 'debug') else ''
env['SERVICE'] = args.service if hasattr(args, 'service') else ''

# debug
#print(args)
#print(env)
#import sys
#sys.exit(0)

# re exec the ./manage.sh, equivalent to `exec ./manage.sh` in shell script
# os.execle(filepath, arg0, arg1, ..., argN, env_dict)
# arg0 is usually the exec path or simply the exec name. For example,
# The path to the Python interpreter
#   path = "/usr/bin/python3"
# Arguments: the first is `arg0`, the rest are passed to the new program
# os.execle(path, "python3", "-c", "print('CRAB!')", {"HOME": "/home/run"})
os.execle(MANAGE_SH_PATH, MANAGE_SH_PATH, env)
