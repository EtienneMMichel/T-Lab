"""
config.py

Configuration module for connectors.
Uses INI configuration format.
"""

import configparser
import os

class Config:
    """
    Config class

    Represent a configuration module.

    The Config class is based on a INI configuration file.
    Parameters are interpreted as strings.

    Methods
    -------
    get(name, section=None)
        Returns the given parameter value.
    set(name, value, section=None)
        Set the given parameter with the given value.
    check()
        Check that every mandatory parameter is defined and is not empty.
    dump()
        Print the whole configuration to console.
    """

    def __init__(self, file_path, mandatory_fields=[]):
        """Create and returns the configuration object.

        Configuration is loaded from the given file. The file content shall match INI configuration format.
        If mandatory fields are given, a check will be performed after loading configuration.

        Parameters
        ----------
        file_path : str
            The path to the configuration file to load.
        mandatory_fields : list
            The list of mandatory fields.

        Returns
        -------
        Config
            The Config object instance.
        """
        self.file = file_path
        self.__parser = configparser.ConfigParser()
        self.__parser.read(file_path)
        self.__mandatory_config = mandatory_fields
        if mandatory_fields:
            (ret, missing_fields) = self.check()
            if not ret:
                raise ValueError("[ERROR] Missing mandatory configuration fields '{}'".format(missing_fields))

    def get(self, name, section=None):
        """Returns the given parameter value.

        If section is not defined, the parameter is searched through whole configuration.
        Otherwise only in the though the specified section.

        Parameters
        ----------
        name : str
            The parameter name
        section : str
            The parameter section.

        Returns
        -------
        str
            The parameter value is the parameter exists, an empty string otherwise
        """
        if section is None:
            for sect in self.__parser.sections():
                if name in self.__parser[sect]:
                    return self.__parser[sect][name]
        else:
            if name in self.__parser[section]:
                return self.__parser[section][name]
        return ""

    def set(self, name, value, section = None):
        """Set the given parameter with the given value.

        If section is not defined, the parameter is saved under 'DEFAULT' section.

        Parameters
        ----------
        name : str
            The parameter name
        value : str
            The parameter value.
        section : str
            The parameter section.
        """
        if section is None:
            self.__parser["DEFAULT"][name] = value
        else:
            self.__parser[section][name] = value

    def check(self):
        """Check that every mandatory parameter is defined and is not empty.

        If section is not defined, the parameter is saved under 'DEFAULT' section.

        Returns
        -------
        bool, list
            True if every parameter is defined, False otherwise
            The list of undefined parameters if False, empty list otherwise.
        """
        ret = True
        missing_conf = []
        for conf in self.__mandatory_config:
            if not self.get(conf):
                missing_conf.append(conf)
                ret = False
        return (ret, missing_conf)

    def dump(self):
        """Print the whole configuration to console.
        """
        for sect in self.__parser.sections():
            print("[{}]".format(sect))
            for key in self.__parser[sect]:
                print("\t'{}' : '{}'".format(key, self.__parser[sect][key]))

__config = None

def load(file_path, mandatory_fields=[]):
    global __config
    if not os.path.isfile(file_path):
        raise OSError("[ERROR] Configuration file '{}' doesn't exist or is not a file".format(file_path))
    __config = Config(file_path, mandatory_fields)

def get(name, section=None):
    #assert __config, "[ERROR] No config loaded"
    global __config
    if __config:
        return __config.get(name, section)
    return ""

def set(name, value, section = None):
    #assert __config, "[ERROR] No config loaded"
    global __config
    if __config:
        __config.set(name, value, section)

def check():
    #assert __config, "[ERROR] No config loaded"
    global __config
    if __config:
        return __config.check()

def dump():
    #assert __config, "[ERROR] No config loaded"
    global __config
    if __config:
        __config.dump()