from .config import (
    SettingsList,
    VALID_SCOPE_VALUES,
    find_start_end_indexes,
    write_content_to_file,
    strip_conflict_markers,
)

# markers
START_MARKER = r'static const ConfigurationOption internal_options\[\] = \{\n'
END_MARKER = r',\s*FINAL_ALIAS};'


# generate the scope code for the ConfigurationOption array and insert into the config file
def generate_scope_code(file):
    with open(file, 'r') as source_file:
        source_code = strip_conflict_markers(source_file.read())

    # find the start and end indexes of the settings' scope array
    start_index, end_index = find_start_end_indexes(source_code, START_MARKER, END_MARKER, file)

    # split source code into sections
    before_array = source_code[:start_index] + "\n    "
    after_array = source_code[end_index:]

    # generate new entries for the settings array
    new_entries = []
    new_aliases = []
    for setting in SettingsList:
        if setting.is_generic_setting:
            if setting.on_set:
                new_entries.append([setting.name, f"DUCKDB_SETTING_CALLBACK({setting.struct_name})"])
            else:
                new_entries.append([setting.name, f"DUCKDB_SETTING({setting.struct_name})"])
        elif setting.scope in VALID_SCOPE_VALUES:  # valid setting_scope values
            new_entries.append([setting.name, f"DUCKDB_{setting.scope}({setting.struct_name})"])
        else:
            raise ValueError(f"Setting {setting.name} has invalid input scope value")
        for alias in setting.aliases:
            new_aliases.append([alias, setting.name])
    new_entries.sort(key=lambda x: x[0])
    new_aliases.sort(key=lambda x: x[0])
    for alias in new_aliases:
        alias.append(f"DUCKDB_SETTING_ALIAS_NAME(\"{alias[0]}\", \"{alias[1]}\")")

    new_array_section = ',\n    '.join([x[1] for x in new_entries])
    new_array_section += ',\n    FINAL_SETTING};\n\n'
    new_array_section += 'static idx_t GetInternalOptionIndexByName(const char *name) {\n'
    new_array_section += '\tfor (idx_t index = 0; internal_options[index].name; index++) {\n'
    new_array_section += '\t\tif (StringUtil::Equals(internal_options[index].name, name)) {\n'
    new_array_section += '\t\t\treturn index;\n'
    new_array_section += '\t\t}\n'
    new_array_section += '\t}\n'
    new_array_section += '\tthrow InternalException("Unknown configuration option alias target \\"%s\\"", name);\n'
    new_array_section += '}\n\n'
    new_array_section += 'static const ConfigurationAlias setting_aliases[] = {'
    new_array_section += ',\n                                                     '.join([x[2] for x in new_aliases])

    return before_array + new_array_section + after_array


def generate():
    from .config import DUCKDB_SETTINGS_SCOPE_FILE

    print(f"Updating {DUCKDB_SETTINGS_SCOPE_FILE}")
    new_content = generate_scope_code(DUCKDB_SETTINGS_SCOPE_FILE)
    write_content_to_file(new_content, DUCKDB_SETTINGS_SCOPE_FILE)


if __name__ == '__main__':
    raise ValueError("Please use 'generate_settings.py' instead of running the individual script(s)")
