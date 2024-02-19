import tomlkit

from .base import register_action, ActionBase


@register_action('backend', 'backend actions', aliases=['b'])
class BackendAction(ActionBase):
    backend_options = {
        ('backend_action', ): {
            'type': str,
            'default': None,
            'choices': ['info', 'kill'],
            'help': 'The action to perform.',
        },
    }

    def __init__(self, name, parser_kwargs):
        super().__init__(name, parser_kwargs)
        self.options.update(self.backend_options)

    def info(self, args):
        binfo = self.backend.backend_info()
        binfo['env'] = self.backend.env
        binfo = {
            'backend': binfo,
            'config': self.backend.config,
        }
        print(tomlkit.dumps(binfo).rstrip())

    def kill(self, args):
        self.backend.backend_kill(args)
        print(f'Killed {self.backend.name} backend.')

    def main(self, args):
        try:
            return getattr(self, args.backend_action)(args)
        except AttributeError:
            print(f'Invalid backend action: {args.backend_action}')
            return 1
