import json


class SQLServerOptions:
    """
    The Subsystem can accept requests either from the facade or client directly.
    In any case, to the Subsystem, the Facade is yet another client, and it's
    not a part of the Subsystem.
    """

    def get_options(self, database, host, user, secret, port=1433, charset="UTF-8", tds_version="7.0") -> str:
        options = {}
        options['charset'] = charset
        options['db'] = database
        options['password'] = secret
        options['port'] = port
        options['server'] = host
        options['tds_version'] = tds_version
        options['user'] = user

        return options