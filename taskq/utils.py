COMMAND_DISPLAY_ESCAPES = {
    '\\': '\\\\',
    '\b': '\\b',
    '\f': '\\f',
    '\n': '\\n',
    '\r': '\\r',
    '\t': '\\t',
    '\v': '\\v',
}


def escape_command_display(command):
    output = []
    for char in command:
        if char in COMMAND_DISPLAY_ESCAPES:
            output.append(COMMAND_DISPLAY_ESCAPES[char])
        elif char.isprintable():
            output.append(char)
        else:
            codepoint = ord(char)
            if codepoint <= 0xff:
                output.append(f'\\x{codepoint:02x}')
            elif codepoint <= 0xffff:
                output.append(f'\\u{codepoint:04x}')
            else:
                output.append(f'\\U{codepoint:08x}')
    return ''.join(output)


def timedelta_format(delta, fmt, num_components):
    total_seconds = delta.total_seconds()
    components = {
        'w': 604800,
        'd': 86400,
        'h': 3600,
        'm': 60,
        's': 1,
    }
    if not all(c in components for c in fmt):
        raise ValueError(f'Invalid format: {fmt}')
    text = []
    leading_zeros = True
    for k, v in components.items():
        if k not in fmt:
            continue
        count = int(total_seconds // v)
        total_seconds -= count * v
        if leading_zeros and not count:
            continue
        leading_zeros = False
        if num_components is not None:
            if len(text) >= num_components:
                continue
        text.append(f'{count}{k}')
    return ''.join(text) or '0s'
