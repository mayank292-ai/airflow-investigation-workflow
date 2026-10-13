"""Shared bounded subprocess launch for manual and evaluation model calls."""
import subprocess
import sys
import os


def execute_bounded(settings, store, inv, question=None, mode='report', timeout_seconds=None):
    if mode not in ('report', 'patch', 'logs-only'):
        raise ValueError('Unknown investigator mode')
    args = [sys.executable, '-m', 'afi.runner', inv, mode]
    if question:
        args.append(question)
    # Carry the admitted settings into the child. Otherwise a bounded evaluation
    # would reload larger .env limits and violate its parent's reservation bound.
    child_env = os.environ.copy()
    for name, value in settings.model_dump(exclude={'root'}).items():
        child_env['AFI_' + name.upper()] = ','.join(value) if name == 'dags' else str(value)
    process = subprocess.Popen(args, cwd=settings.root, env=child_env)
    try:
        return process.wait(timeout=min(settings.investigation_seconds + 10, timeout_seconds)
                            if timeout_seconds is not None else settings.investigation_seconds + 10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)
        store.status(inv, 'interrupted')
        store.operation(inv, 'failure', 'interrupted', {'reason': 'Parent deadline terminated operation'})
        return 1
