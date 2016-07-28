#!/usr/bin/env python3

import click
import os
from xmlrpc.client import ServerProxy

import sys
from os.path import dirname
sys.path.append(dirname(dirname(__file__)))
import yadfs.client.client
import yadfs.utils.enums


class Client:
    def __init__(self):
        self.fs = yadfs.client.client.Client()
        if not os.getenv('YAMR_JT'):
            os.environ['YAMR_JT'] = 'http://localhost:11111'
        self.jt = ServerProxy(os.environ['YAMR_JT'])

    def start_task(self, inp, map_script, reduce_script):
        return self.jt.create_task(inp, map_script, reduce_script)

    def upload(self, path, remote_path):
        return self.fs.create_file(path, remote_path)


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


@cli.command()
@click.argument('local_path')
@click.argument('remote_path', default="/")
def upload(local_path, remote_path):
    """Create a file"""
    cl = Client()

    if os.path.isdir(local_path):
        print("You can't upload directory as a file")
    else:
        res = cl.upload(local_path, remote_path)
        stat = res['status']
        if stat != yadfs.utils.enums.Status.ok:
            print(yadfs.utils.enums.Status.description(stat))

if __name__ == '__main__':
    cli()
