import json


class SpreadSheetOptions:
    """
    The Subsystem can accept requests either from the facade or client directly.
    In any case, to the Subsystem, the Facade is yet another client, and it's
    not a part of the Subsystem.
    """

    def get_options(self, database, host, user, secret) -> str:
        options = {}
        options['jsonKeyFile'] = host

        return options