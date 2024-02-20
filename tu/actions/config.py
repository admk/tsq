import tomlkit

from .base import register_action, ActionBase


@register_action('config', 'config action', aliases=['c'])
class ConfigAction(ActionBase):
    config_options = {
        ('key', ): {
            'type': str,
            'nargs': '?',
            'help': 'The key to get/set.',
        },
        ('value', ): {
            'type': str,
            'nargs': '?',
            'help': 'The value to set.',
        },
    }

    def __init__(self, name, parser_kwargs):
        super().__init__(name, parser_kwargs)
        self.options.update(self.config_options)

    def getset(self, mapping, key, value=None):
        *key_parts, last_key = key.split('.')
        for k in key_parts:
            mapping = mapping.setdefault(k, {})
        if value is None:
            return mapping.get(last_key)
        mapping[last_key] = value

    def main(self, args):
        config = self.backend.config
        if args.key is None:
            print(tomlkit.dumps(config).rstrip())
            return
        if args.value is None:
            print(self.getset(config, args.key))
            return
        self.getset(config, args.key, args.value)
        with open('.tu.toml', 'w', encoding='utf-8') as f:
            f.write(tomlkit.dumps(config))
