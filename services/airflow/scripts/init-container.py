"""Initialize Airflow and FAB without exposing bootstrap credentials in process arguments."""
import json
from pathlib import Path
import subprocess


def run(*arguments):
    subprocess.run(['airflow', *arguments], check=True)


run('db', 'migrate')
run('fab-db', 'migrate')

from airflow.providers.fab.auth_manager.cli_commands.user_command import get_application_builder
from werkzeug.security import check_password_hash

credentials = json.loads(Path('/run/secrets/admin_credentials').read_text())
with get_application_builder() as builder:
    manager = builder.sm
    admin = manager.find_role('Admin')
    if admin is None:
        raise RuntimeError('FAB Admin role is unavailable')
    user = manager.find_user(username=credentials['username'])
    if user is None:
        user = manager.add_user(credentials['username'], 'Lab', 'Operator',
                                'operator@localhost.invalid', admin, credentials['password'])
        if not user:
            raise RuntimeError('Unable to create the Airflow administrator')
    elif not check_password_hash(user.password, credentials['password']) or admin not in user.roles:
        raise RuntimeError('Existing administrator differs from retained bootstrap credentials')

run('pools', 'set', 'export_jobs', '1', 'One bounded warehouse export task at a time')
run('pools', 'set', 'default_pool', '1', 'Single-VM default task capacity')
print('Airflow metadata, administrator and task pools initialized.')
