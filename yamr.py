#!/usr/bin/env python3

import click
import os
import time
from xmlrpc.client import ServerProxy

import sys
from os.path import dirname
from enums import TaskStatus

sys.path.append(dirname(dirname(__file__)))
import yadfs.client.client
import yadfs.utils.enums


class Client:
    def __init__(self):
        self.fs = yadfs.client.client.Client()
        if not os.getenv('YAMR_JT'):
            os.environ['YAMR_JT'] = 'http://localhost:11111'
        self.jt = ServerProxy(os.environ['YAMR_JT'])

    def start_task(self, inp, script):
        return self.jt.create_task(inp, script)

    def upload(self, path, remote_path):
        return self.fs.create_file(path, remote_path)

    def get_status(self, task_id):
        return self.jt.get_status(task_id)

    def get_result(self, task_id):
        return self.jt.get_result(task_id)


# CLI
@click.group(invoke_without_command=False)
@click.pass_context
def cli(ctx):
    pass


@cli.command()
@click.argument('path')
@click.argument('script')
def start_task(path, script):
    """Start new task"""
    cl = Client()
    task_id = cl.start_task(path, script)
    status = cl.get_status(task_id)
    while status != TaskStatus.task_done:
        time.sleep(0.5)
        status = cl.get_status(task_id)

    res = cl.get_result(task_id)

    print("Paths with result:")
    for path in res:
        print(path)


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
