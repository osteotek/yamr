#!/usr/bin/env python3

import click
import os
from xmlrpc.client import ServerProxy


class Client:
    def __init__(self):
        if not os.getenv('YAMR_JT'):
            os.environ['YAMR_JT'] = 'http://localhost:11111'
        self.jt = ServerProxy(os.environ['YAMR_JT'])

    def start_task(self, inp, map_script, reduce_script):
        return self.jt.create_task(inp, map_script, reduce_script)


# CLI
@click.group(invoke_without_command=False)
@click.pass_context
def cli(ctx):
    pass


@cli.command()
@click.argument('path')
@click.argument('map_script')
@click.argument('reduce_script')
def start_task(path, map_script, reduce_script):
    """Start new task"""
    cl = Client()
    res = cl.start_task(path, map_script, reduce_script)
    print(res)

if __name__ == '__main__':
    cli()
