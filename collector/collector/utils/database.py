"""
database.py

Database module for Myfo.
"""

from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import threading
import pymysql

class Table():
    '''
    Table class

    Represent a static table linked to a Database class.

    The table is extracted from database by issuing SQL queries through 'pymysql'.
    The extraction is made at the first retrievement of the table.
    Once the table is loaded, it's formatted into a pandas Dataframe.

    Methods
    -------
    getDataframe()
        Return the formatted table dataframe.
    getColumns()
        Returns table columns as a list.
    '''
    __db = None
    __name = ""

    def __init__(self, database, name):
        """
        Parameters
        ----------
        database : Database
            The database from which the table is extracted.
        name : str
            The table name.
        """
        self.__name = name
        self.__db = database

    def getDataframe(self, columns=None, arg="", headers=None):
        """Return the formatted table dataframe.

        Parameters
        ----------
        columns : list, tuple or string
            The columns to be extracted.
            If a string is given columns shall be comma-separated.
        arg : str
            Additionnal arguments to pass to SQL query.
        header : list
            Names of dataframe columns. If empty uses 'columns' names

        Returns
        -------
        pandas.DataFrame
            The dataframe associated to the table
        """
        select = ""
        if columns:
            if isinstance(columns, list) or isinstance(columns, tuple):
                for column in columns:
                    select += column + ","
                select = select[:-1]
            if isinstance(columns, str):
                select = columns
                columns = columns.replace(" ", "").split(",")
            if headers:
                columns = headers
        else:
            select = "*"
            columns = self.getColumns()

        rawDataframe = self.__db.extract(self.__name, select, arg)
        dataframe = pd.DataFrame(list(rawDataframe), columns=columns)
        return dataframe

    def getColumns(self):
        """Returns table columns as a list.

        Returns
        -------
        list
            The table's column list
        """
        return [column[0] for column in self.__db.execute("SHOW columns FROM {}".format(self.__name), fetch=True)]

class Database():
    '''
    Database class

    Represent a database.

    Methods
    -------
    setTables(tables)
        Set the given dictionary as the database table list
    getTable(table)
        Extract the given table and returns its dataframe.
    connect()
        Connects to database.
    disconnect()
        Disconnects from database.
    execute(query, fetch)
        Stack an SQL query to be sent.
    extract()
        "Extract data from table using composed query.
    '''
    isConnected = False
    engine = None
    dbName = ""
    __ip = None
    __user = None
    __password = None
    __connection = None
    __cursor = None

    def __init__(self, ip, user, password, dbName):
        """
        Parameters
        ----------
        ip : str
            IP address of the mySQL server.
        user : str
            The user name to access server.
        password : str
            The user password.
        dbName : str
            The name of the database.
        """
        self.__ip = ip
        self.__user = user
        self.__password = password
        self.__mutex = threading.Lock()
        self.dbName = dbName

        self.engine = create_engine("mysql+pymysql://{user}:{pw}@{ip}/{db}"
            .format(user=self.__user,
                    pw=self.__password,
                    ip=self.__ip,
                    db=self.dbName))

    def getTable(self, table, columns=None, arg="", headers=None):
        """Extract the given table and returns its dataframe.

        Parameters
        ----------
        table : str
            The table to extract.
        columns : list, tuple or string
            The columns to be extracted.
            If a string is given columns shall be comma-separated.
        arg : str
            Additionnal arguments to pass to SQL query.
        header : list
            Names of dataframe columns. If empty uses 'columns' names

        Returns
        -------
        pandas.DataFrame
            The table dataframe.
        """
        return Table(self, table).getDataframe(columns=columns, arg=arg, headers=headers)

    def append_to_table(self, table, dataframe):
        self.__mutex.acquire()
        try:
            dataframe.to_sql(table, con=self.engine, if_exists='append', index=False)
        finally:
            self.__mutex.release()

    def getTableChunked(self, table):
        columns = [column[0] for column in self.execute("SHOW columns FROM {}".format(table), fetch=True)]
        df = pd.DataFrame(list(self.extract(table, "*")), columns=columns)
        print(df.dtypes)

    def connect(self):
        """Connects to database.
        """
        self.__connect()

    def disconnect(self):
        """Disconnects from database.
        """
        self.__disconnect()

    def execute(self, query, params=None, fetch=False):
        """Execute and commit an SQL query.

        Parameters
        ----------
        query : str
            The query to execute.
        params : tuple or list
            The params passed as values to the query.
        fetch : bool
            If 'True', fetch and returns data. Defaults to 'False'.

        Returns
        -------
        tuple
            The tuple associated to the query if fetch is 'True'. Nothing otherwise
        """

        self.__mutex.acquire()
        try:
            # Save initial connection status
            connection_status = self.isConnected

            # Connect
            self.__connect()

            # Execute SDL query
            if params:
                self.__cursor.execute(query, params)
            else:
                self.__cursor.execute(query)
            self.__connection.commit()

            # Reset connection to initial state
            if not connection_status:
                self.__disconnect()

            if fetch:
                return self.__cursor.fetchall()
        finally:
            self.__mutex.release()

    def extract(self, table, select, arg=""):
        """Extract data from table using composed query.

        Parameters
        ----------
        table : str
            The table name.
        select : str
            Fields selection into table (comma separated).
        arg : str
            Query arguments. Unused by default.

        Returns
        -------
        tuple
            The tuple associated to the query.
        """

        self.__mutex.acquire()
        try:
            # Save initial connection status
            connection_status = self.isConnected

            # Connect
            self.__connect()

            # Execute SQL query
            sql_query = "SELECT {} FROM {} {}".format(select, table, arg)
            self.__cursor.execute(sql_query)

            # Reset connection to initial state
            if not connection_status:
                self.__disconnect()

            # Returns query tuple
            return self.__cursor.fetchall()
        finally:
            self.__mutex.release()

    def __connect(self):
        if not self.isConnected:
            self.__connection = pymysql.connect(host=self.__ip, user=self.__user, password=self.__password, database=self.dbName)
            self.__cursor = self.__connection.cursor()
            pymysql.converters.encoders[np.float64] = pymysql.converters.escape_float
            pymysql.converters.conversions = pymysql.converters.encoders.copy()
            pymysql.converters.conversions.update(pymysql.converters.decoders)
            self.isConnected = True

    def __disconnect(self):
        if self.isConnected:
            self.__cursor.close()
            self.__connection.close()
            self.isConnected = False
