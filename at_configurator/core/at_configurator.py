import asyncio

from at_config.core.at_config_handler import ATComponentConfig
from at_config.core.at_config_loader import load_config
from at_queue.core.at_component import ATComponent
from at_queue.utils.decorators import authorized_method
from at_queue.utils.decorators import component_method


class ATConfigurator(ATComponent):
    @component_method
    async def configurate(self, config: dict, auth_token: str = None, *args, **kwargs):
        config = await load_config(config)
        return await asyncio.gather(
            *[self.send_configurate(component, config, auth_token=auth_token) for component, config in config.items()]
        )

    @authorized_method
    async def authorized_configurate(self, config: dict, auth_token: str = None, *args, **kwargs):
        config = await load_config(config)
        return await asyncio.gather(
            *[self.send_configurate(component, config, auth_token=auth_token) for component, config in config.items()]
        )

    async def send_configurate(self, component: str, config: ATComponentConfig, auth_token: str = None):
        if not await self.check_external_registered(component):
            raise ReferenceError(f'Component "{component}" is not registered')
        return await self.session.send_await(
            component,
            message={
                "type": "configurate",
                "config": config.__dict__,
            },
            auth_token=auth_token,
        )
