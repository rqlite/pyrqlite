
import collections


def select_identifier_map(sql_str):
    """
    Given a SELECT statement, map result columns to the table/column
    that they were selected from.
    """

    # lazy import, so sqlparse is optional
    import sqlparse
    from sqlparse.sql import (
        Identifier,
        IdentifierList,
    )

    result_fields = []
    tables = []
    result_aliases = {}
    table_aliases = {}

    statement = sqlparse.parse(sql_str)[0]
    tokens = list(statement.tokens)
    tokens.reverse()
    while tokens:
        token = tokens.pop()
        value_upper = token.value.upper()
        if value_upper in ('FROM', 'JOIN') or value_upper.endswith(' JOIN'):
            if tokens and tokens[-1].is_whitespace():
                tokens.pop()
                if tokens:
                    identifiers = None
                    if isinstance(tokens[-1], Identifier):
                        identifiers = [tokens.pop()]
                    elif isinstance(tokens[-1], IdentifierList):
                        identifiers = tokens.pop().get_identifiers()

                    if identifiers is not None:
                        for identifier in identifiers:
                            tables.append(identifier.get_real_name())
                            if identifier.has_alias():
                                table_aliases[identifier.get_name()] = identifier.get_real_name()

        elif value_upper == 'SELECT':
            if tokens and tokens[-1].is_whitespace():
                tokens.pop()
                if tokens:
                    # TODO: expand '*'
                    identifiers = None
                    if isinstance(tokens[-1], Identifier):
                        identifiers = [tokens.pop()]
                    elif isinstance(tokens[-1], IdentifierList):
                        identifiers = tokens.pop().get_identifiers()

                    if identifiers is not None:
                        for identifier in identifiers:
                            parent_name = identifier.get_parent_name()
                            if parent_name is not None:
                                tables.append(parent_name)
                            result_fields.append(identifier.get_name())
                            if identifier.has_alias():
                                result_aliases[identifier.get_name()] = (parent_name, identifier.get_real_name())

        unique_tables = set(tables)
        for table in list(unique_tables):
            real_name = table_aliases.get(table)
            if real_name is not None:
                unique_tables.remove(table)
                unique_tables.add(real_name)

    mapping = collections.OrderedDict()
    if result_fields:
        default_table = None
        if len(unique_tables) == 1:
            default_table = next(iter(unique_tables))
        for field in result_fields:
            real_name = result_aliases.get(field)
            parent = None
            if real_name is not None:
                parent, name = real_name
            else:
                name = field
            if parent is None:
                parent = default_table
            else:
                parent = table_aliases.get(parent, parent)
            mapping[field] = (parent, name)

    return mapping
