import copy
import tomlkit

from ..common import dict_simplify
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
            'help': 'The value to set.  Use "null" to delete.',
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
            bvalue = self.backend.backend_getset(key)
            if bvalue is not None:
                return bvalue
            return mapping.get(last_key)
        if value == 'null':
            del mapping[last_key]
        else:
            mapping[last_key] = value
        self.backend.backend_getset(key, value)

    def main(self, args):
        config = dict_simplify(copy.deepcopy(self.backend.config))
        if args.key is None:
            print(tomlkit.dumps(config).rstrip())
            return
        if args.value is None:
            print(self.getset(config, args.key))
            return
        with open(args.rc_file, 'r', encoding='utf-8') as f:
            rc_config = tomlkit.load(f)
        self.getset(rc_config, args.key, args.value)
        with open(args.rc_file, 'w', encoding='utf-8') as f:
            f.write(tomlkit.dumps(rc_config))
