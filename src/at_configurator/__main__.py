import argparse
from at_queue.core.session import ConnectionParameters
from at_configurator.core.at_configurator import ATConfigurator
from at_config.core.at_config_loader import CONFIG_SCHEMA, Or, Schema, Use, And, Optional
import asyncio
import logging
import json
import yaml

parser = argparse.ArgumentParser(
    prog='at-temporal-configurator',
    description='General working memory for AT-TECHNOLOGY components')

parser.add_argument('-u', '--url', help="RabbitMQ URL to connect", required=False, default=None)
parser.add_argument('-H', '--host', help="RabbitMQ host to connect", required=False, default="localhost")
parser.add_argument('-p', '--port', help="RabbitMQ port to connect", required=False, default=5672)
parser.add_argument('-L', '--login', '-U', '--user', '--user-name', '--username', '--user_name', dest="login", help="RabbitMQ login to connect", required=False, default="guest")
parser.add_argument('-P', '--password', help="RabbitMQ password to connect", required=False, default="guest")
parser.add_argument('-v',  '--virtualhost', '--virtual-host', '--virtual_host', dest="virtualhost", help="RabbitMQ virtual host to connect", required=False, default="/")

parser.add_argument('-c',  '--config', dest="config", help="Initial configuration file", required=False, default=None)


CONFIG_DATA_SCHEMA = Or(
    And(
        CONFIG_SCHEMA,
        Use(lambda value: {'config': value})
    ),
    Schema({
        Optional('auth_token'): str,
        'config': CONFIG_SCHEMA
    }, ignore_extra_keys=True)
)

async def apply_configuration(config_data: dict, configurator: ATConfigurator):
    auth_token = config_data.get('auth_token', None)
    config = config_data.get('config', {})
    await configurator.configurate(config, auth_token=auth_token)

async def main(config: str = None, **connection_kwargs):
    connection_parameters = ConnectionParameters(**connection_kwargs)
    configurator = ATConfigurator(connection_parameters=connection_parameters)
    await configurator.initialize()
    await configurator.register()

    loop = asyncio.get_event_loop()
    task = loop.create_task(configurator.start())

    if config is not None:
        if config.endswith('.yml') or config.endswith('.yaml'):
            data = yaml.safe_load(open(config))
        elif config.endswith('.json'):
            data = json.load(open(config))
        else:
            raise ValueError(f'Invalid config file {config}, expected yaml, yml or json')
        config_data = CONFIG_DATA_SCHEMA.validate(data)
        await apply_configuration(config_data, configurator)

    await task


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    args = parser.parse_args()
    args_dict = vars(args)
    config_file = args_dict.pop('config', None)
    asyncio.run(main(**args_dict, config=config_file))