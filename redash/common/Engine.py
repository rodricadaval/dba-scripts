from __future__ import annotations
import MongoDBOptions
import MySQLOptions
import RDSMySQLOptions
import PostgreSQLOptions
import PrometheusOptions
import SpreadSheetOptions
import SQLServerOptions
import MsSQLODBCOptions
import MsSQLOptions
import InfluxDBOptions
import ElasticSearchOptions
import RedShiftOptions


class EngineAbstract:
    """
    The Facade class provides a simple interface to the complex logic of one or
    several subsystems. The Facade delegates the client requests to the
    appropriate objects within the subsystem. The Facade is also responsible for
    managing their lifecycle. All of this shields the client from the undesired
    complexity of the subsystem.
    """

    def __init__(self, p_type) -> None:
        """
        Depending on your application's needs, you can provide the Facade with
        existing subsystem objects or force the Facade to create them on its
        own.
        """
        self._type_description = p_type
        if p_type in ["pg", "postgres"]:
            self._type = PostgreSQLOptions.PostgreSQLOptions()
        elif p_type == "mysql":
            self._type = MySQLOptions.MySQLOptions()
        elif p_type == "mongodb":
            self._type = MongoDBOptions.MongoDBOptions()
        elif p_type == "sqlserver":
            self._type = SQLServerOptions.SQLServerOptions()
        elif p_type == "redshift":
            self._type = RedShiftOptions.RedShiftOptions()
        elif p_type == "mssql":
            self._type = MsSQLOptions.MsSQLOptions()
        elif p_type == "google_spreadsheets":
            self._type = SpreadSheetOptions.SpreadSheetOptions()
        elif p_type == "mssql_odbc":
            self._type = MsSQLODBCOptions.MsSQLODBCOptions()
        elif p_type == "rds_mysql":
            self._type = RDSMySQLOptions.RDSMySQLOptions()
        elif p_type == "prometheus":
            self._type = PrometheusOptions.PrometheusOptions()
        elif p_type == "elasticsearch":
            self._type = ElasticSearchOptions.ElasticSearchOptions()
        elif p_type == "influxdb":
            self._type = InfluxDBOptions.InfluxDBOptions()
        else:
            self._type = self

    def get_options(self, database, host, user, secret, port="") -> str:
            options = {}
            options['dbname'] = database
            options['host'] = host
            options['user'] = user
            options['passwd'] = secret
            options['port'] = port

            return options

    @property
    def get_type(self):
        return self._type