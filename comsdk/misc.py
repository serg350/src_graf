import shutil
from functools import reduce, partial
import os
import re
import collections
from copy import deepcopy
import importlib
from abc import ABC, abstractmethod
from typing import Optional, List, Tuple, Type, Any
import json

import numpy as np
from jsons import JsonSerializable

ArrayItemGetter = collections.namedtuple('ArrayItemGetter', ['key_path_to_array', 'i'])


class StandardisedNaming(ABC):
    """
    Class StandardisedNaming is an abstract class used to represent standardised names of files and directories in a
    general sense. To make use of its features, one needs to derive one's own class and implement methods
    regexp_with_substitutions and make_name. The former should return a group-named regular expression (with or
    without substitution) which can be used to recognise whether a concrete name corresponds to the standardised name
    or not. The latter should create a concrete standardised name based on its attributes.
    """

    @classmethod
    def regexp(cls) -> str:
        """
        Returns a full group-named regular expression which can be used to determine whether a certain name follows the
        standardised naming.

        :return: regular expression as a string
        """
        return cls.regexp_with_substitutions()

    @classmethod
    def parse(cls, name: str) -> Optional[dict]:
        """
        Checks whether a given name follows the standardised naming and, if yes, parses the name and returns a
        dictionary of its attributes.

        :param name: name to be parsed
        :return: either dictionary of the name attributes or None if a given name does not follow the standardised
                 naming
        """
        return parse_by_named_regexp(cls.regexp(), name)

    @classmethod
    @abstractmethod
    def regexp_with_substitutions(cls, **kwargs) -> str:
        """
        Returns a group-named regular expression (if kwargs are given, they will substituted to the regular expression
        according to the names) which can be used to recognise whether a concrete name follows the standardised naming
        or not.

        :param kwargs: name attributes
        :return: regular expression as a string
        """
        raise NotImplementedError('Must be implemented. It must return the regular expression with substitutions based '
                                  'on kwargs arguments. Being invoked with no arguments, it must return the full '
                                  'regular expression')

    @classmethod
    @abstractmethod
    def make_name(cls, **kwargs) -> str:
        """
        Returns name based on the standardised naming and attributes passed via kwargs.

        TODO: must be implemented (or joint with regexp_with_substitutions) such that regexp_with_substitutions is
              used inside it

        :param kwargs: name attributes
        :return: name as a string
        """
        raise NotImplementedError('Must be implemented. It must return the name using kwards arguments as '
                                  'substitutions')


class ProxyDict(object):
    '''
    Class allowing to access a dict via a proxy mapping using the same interface as dict does.
    It supports two types of proxy mappings:
    1) relative_keys
    2) keys_mappings
    and also extends a simple key to key_path. For example, a sequence of keys leading to d['a']['b']['c']
    corresponds to a key_path ('a', 'b', 'c').
    Proxy mapping relative_keys is a sequence of key_path leading to subdicts. The content of these subdicts
    is treated as located in the root of the proxy dict. For example, suppose we have d = {'a': 1, 'b':{'c': 2, 'd': 3}}.
    A proxy dict with relative_key ('b',) shall be pd = {'a': 1, 'c': 2, 'd': 3, 'b':{'c': 2, 'd': 3}}.
    Proxy mapping keys_mappings is a dict linking a (new) key in the root of proxy dict to key_path in original dict.
    For example, for dict d, a proxy dict with keys_mappings {'d': ('b', 'd')} shall be pd = {'a': 1, 'd': 3, 'b':{'c': 2, 'd': 3}}.
    Finally, we have default_relative_key which is a key_path leading to a subdict to which new elements must be added.
    For example, for dict d, proxy dict pd and default_relative_key ('b',), operation pd['z'] = 0 leads to the following change in d:
    d = {'a': 1, 'b':{'c': 2, 'd': 3, 'z': 0}}
    The order of the proxy mappings (the higher mapping overwrites the lower):
    1) keys_mappings
    2) relative_keys
    3) original dict (root)
    '''
    def __init__(self, data,
                 relative_keys=(),
                 keys_mappings={},
                 default_relative_key=(),
                 ):
        self._data = data
        self._default_relative_key = list(default_relative_key)
        self._keys_mappings = {key: key for key in self._data.keys()}
        for rel_key in relative_keys:
            for inner_key in recursive_get(data, rel_key).keys():
                self._keys_mappings[inner_key] = list(rel_key) + [inner_key]
        self._keys_mappings.update(keys_mappings)

    def __repr__(self):
        res = '{'
        for key in self._keys_mappings.keys():
            res += '{}: {}, '.format(key, self.__getitem__(key))
        return res + '}'

    def __contains__(self, key):
        return key in self._keys_mappings.keys()

    def __getitem__(self, key):
        # x[key] => x.__getitem__(key)
        return recursive_get(self._data, self._keys_mappings[key])

    def __setitem__(self, key, value):
        # x[key] = value => x.__setitem__(key, value)
        if key in self._keys_mappings:
            recursive_set(self._data, self._keys_mappings[key], value)
        else:
            recursive_set(self._data, self._default_relative_key + [key], value)
            self._keys_mappings[key] = self._default_relative_key + [key]

    def __delitem__(self, key):
        # del x[key] => x.__delitem__(key)
        val = recursive_get(self._data, self._keys_mappings[key])
        del val

    def update(self, mapping):
        for key in mapping.keys():
            self.__setitem__(key, mapping[key])

def recursive_get(d, keys):
    if isinstance(keys, ArrayItemGetter):
         array_ = recursive_get(d, keys.key_path_to_array)
         return array_[keys.i]
    elif is_sequence(keys):
        return reduce(lambda d_, key_: d_.get(key_, {}), keys, d)
    else:
        return d[keys]

def recursive_set(d, keys, val):
    if isinstance(keys, ArrayItemGetter):
        array_ = recursive_get(d, keys.key_path_to_array)
        array_[keys.i] = val
    elif is_sequence(keys):
        last_dict = reduce(lambda d_, key_: d_.setdefault(key_, {}), keys[:-1], d)
        last_dict[keys[-1]] = val
    else:
        d[keys] = val

def is_sequence(obj):
    '''
    Checks whether obj is a sequence (string does not count as a sequence)
    '''
    return isinstance(obj, collections.abc.Sequence) and (not hasattr(obj, 'strip'))

def cp(from_, to_):
    '''
    Copies from_ to to_ where from_ may be file or dir and to_ is a dir.
    Returns new path.
    '''
    if os.path.isfile(from_):
        shutil.copy(from_, to_)
    else:
        shutil.copytree(from_, to_)
        return os.path.join(to_, os.path.basename(from_))

def rm(target):
    '''
    Removes target which may be file or dir.
    '''
    if os.path.isfile(target):
        os.remove(target)
    else:
        shutil.rmtree(target)

def remove_if_exists(path):
    try:
        os.remove(path)
        return True
    except FileNotFoundError as e:
        return False

def create_file_mkdir(filepath):
    '''
    Opens a filepath in a write mode (i.e., creates/overwrites it). If the path does not exists,
    subsequent directories will be created.
    '''
    dirpath = os.path.dirname(filepath)
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)
    return open(filepath, 'w')

def merge_dicts(*dict_args):
    '''
    Given any number of dicts, shallow copy and merge into a new dict,
    precedence goes to key value pairs in latter dicts.
    Source: Aaron Hall, https://stackoverflow.com/questions/38987/how-to-merge-two-dictionaries-in-a-single-expression
    '''
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result

def append_code(obj, obj_funcs, code_appendix):
    '''
    Adds the code defined by the function code_appendix in the end of the method obj_funcs of the object obj.
    '''
    def extended_func(func, *args, **kwds):
        func(*args, **kwds)
        code_appendix(*args, **kwds)

    for func_name in obj_funcs:
        func = getattr(obj, func_name)
        if not func:
            raise Exception('Function {} not found'.format(func_name))
        setattr(obj, func_name, partial(extended_func, func))

def do_atomic(proc_func, cleanup_func):
    '''
    Executes the function proc_func such that if an expection is raised, the function cleanup_func
    is executes and only after that the expection is hand over further. It is useful when proc_func
    creates something which should be removed in the case of emergency.
    '''
    try:
        proc_func()
    except Exception as err:
        cleanup_func()
        raise err

def make_atomic(proc_func, cleanup_func):
    '''
    Returns a function corresponding to do_atomic() to which proc_func and cleanup_func are passed.
    '''
    return partial(do_atomic, proc_func, cleanup_func)

def find_dir_by_named_regexp(regexp, where):
    '''
    Search for dir in where which satisfies regexp. If successful, parses the dir according to named regexp.
    Returns a tuple (found_dir, params_from_named_regexp) or None if not found.

    TODO: depricated (see find_dir_by_standardised_name)
    '''
    dirnames = next(os.walk(where))[1]
    for dir_ in dirnames:
        parsing_params = parse_by_named_regexp(regexp, dir_)
        if parsing_params is not None:
            return dir_, parsing_params
    return None

def find_all_dirs_by_named_regexp(regexp, where):
    '''
    Search for dirs in where which satisfies regexp. If successful, parses them according to named regexp.
    Returns a list of tuples (found_dir, params_from_named_regexp).

    TODO: depricated (see find_all_dirs_by_standardised_name)
    '''
    dirnames = next(os.walk(where))[1]
    datas = []
    for dir_ in dirnames:
        parsing_params = parse_by_named_regexp(regexp, dir_)
        if parsing_params is not None:
            datas.append((dir_, parsing_params))
    return datas

def find_all_files_by_named_regexp(regexp, where):
    '''
    Search for files in where which satisfies regexp. If successful, parses them according to named regexp.
    Returns a list of tuples (found_dir, params_from_named_regexp).

    TODO: depricated (see find_all_files_by_standardised_name)
    '''
    filenames = next(os.walk(where))[2]
    datas = []
    for file_ in filenames:
        parsing_params = parse_by_named_regexp(regexp, file_)
        if parsing_params is not None:
            datas.append((file_, parsing_params))
    return datas

def find_dir_by_standardised_naming(naming: Type[StandardisedNaming], where: str) -> Optional[Tuple[str, dict]]:
    '''
    Search for dir in where which satisfies regexp. If successful, parses the dir according to named regexp.
    Returns a tuple (found_dir, params_from_named_regexp) or None if not found.
    '''
    dirnames = next(os.walk(where))[1]
    for dir_ in dirnames:
        parsing_params = naming.parse(dir_)
        if parsing_params is not None:
            return dir_, parsing_params
    return None

def find_all_dirs_by_standardised_naming(naming: Type[StandardisedNaming], where: str) -> List[Tuple[str, dict]]:
    '''
    Search for dirs in where which satisfies regexp. If successful, parses them according to named regexp.
    Returns a list of tuples (found_dir, params_from_named_regexp).
    '''
    dirnames = next(os.walk(where))[1]
    datas = []
    for dir_ in dirnames:
        parsing_params = naming.parse(dir_)
        if parsing_params is not None:
            datas.append((dir_, parsing_params))
    return datas

def find_all_files_by_standardised_naming(naming: Type[StandardisedNaming], where: str) -> List[Tuple[str, dict]]:
    '''
    Search for files in where which satisfies regexp. If successful, parses them according to named regexp.
    Returns a list of tuples (found_dir, params_from_named_regexp).
    '''
    filenames = next(os.walk(where))[2]
    datas = []
    for file_ in filenames:
        parsing_params = naming.parse(file_)
        if parsing_params is not None:
            datas.append((file_, parsing_params))
    return datas

def parse_by_named_regexp(regexp, val):
    '''
    Parses val according to named regexp. Return a dictionary of params.
    '''
    matching = re.search(regexp, val)
    if matching is None:
        return None
    return matching.groupdict()

def parse_datafile(path, data_names, transform_funcs, cols_to_parse=[]):
    '''
    Parses a data file given by path and structured as a table where rows are separated by \n
    and columns are separated by any of whitespaces. The first line in the file will be ignored.
    Processed columns are given by cols_to_parse (all columns will be processed if it is empty).
    Corresponding names and transformation functions for columns in cols_to_parse are given by 
    data_names and transform_funcs. Transformation function must be a mapping string -> type.
    
    Returns a dictionary where a key corresponds to a column name (i.e., taken from data_names)
    and a value corresponds to a list of the columns values taken from all rows.
    '''
    if cols_to_parse == []:
        cols_to_parse = range(len(data_names))
    if len(data_names) != len(transform_funcs) or len(data_names) != len(cols_to_parse):
        raise Exception('Number of data names, transform functions and columns to be parsed is inconsistent')
    data = collections.OrderedDict()
    for data_name in data_names:
        data[data_name] = []

    f = open(path, 'r') # if not found, expection will be raised anyway
    lines = f.readlines()
    for line in lines[1:]: # skip the first line
        tmp = line.split()
        if len(tmp) < len(data_names):
            raise Exception('Number of given data names is larger than number of columns we have in the data file.')
        for i, data_name in enumerate(data_names):
            val = tmp[cols_to_parse[i]]
            data[data_name].append(transform_funcs[i](val))
    return {name: np.array(array_) for name, array_ in data.items()}

def parse_timed_numdatafile(path):
    '''
    Parses a data file given by path and structured as a table where rows are separated by \n
    and columns are separated by any of whitespaces. The table here has an interpretation of a matrix whose 
    rows axis corresponds to time axis and columns axis corresponds to data axis. Moreover, the first column
    contains the time values so the data is contained in columns starting from the second one.

    Returns time_list (a list of times from the first column) and data_matrix (a list of numpy arrays of data where
    list's index corresponds to the time index). 
    '''
    time = []
    data = []
    f = open(path, 'r') # if not found, expection will be raised anyway
    lines = f.readlines()
    for line in lines[1:]: # skip the first line
        tmp = line.split()
        time.append(float(tmp[0]))
        timed_data = np.zeros((len(tmp) - 1, ))
        for i, val in enumerate(tmp[1:]):
            timed_data[i] = float(val)
        data.append(timed_data)
    return time, np.array(data)

def write_datafile(path, data):
    keys = list(data.keys())
#    print(keys)
    values = list(data.values())
    with open(path, 'w') as f:
        f.write(r'% ' + '\t'.join(keys) + '\n')
        for t_i in range(len(values[0])):
            line = '\t'.join([str(array[t_i]) for array in values]) + '\n'
            f.write(line)

def write_timed_numdatafile(path, time, data):
    with open(path, 'w') as f:
        for i in range(len(time)):
            line = '{}\t'.format(time[i]) + '\t'.join([str(data[i][j]) for j in range(data.shape[1])]) + '\n'
            f.write(line)

def load_function_from_module(full_function_name):
    module_name, function_name = full_function_name.rsplit('.', 1)
    module_ = importlib.import_module(module_name)
    return getattr(module_, function_name)

def print_pretty_dict(d):
    for k, v in d.items():
        print('{}: {}'.format(k ,v))


def raise_exception_if_arguments_not_in_keywords_or_none(argument_names, kwargs) -> None:
    for arg in argument_names:
        if arg not in kwargs:
            raise ValueError('Keywords "{} = ..." must be set'.format(arg))
        else:
            if kwargs[arg] is None:
                raise ValueError('Keywords "{}" must not be None'.format(arg))


def take_value_if_not_none(value, default=None, transform=str) -> Any:
    if value is None:
        if default is None:
            raise ValueError('Value must not be None or default must be set')
        else:
            return default
    else:
        return transform(value)


def take_value_by_index(seq, i, default=None) -> Any:
    return seq[i] if seq is not None else default


def dump_to_json(obj: JsonSerializable, path_to_jsons: str = 'jsons') -> None:
    filename = '{}.{}.json'.format(type(obj).__module__, type(obj).__name__)
    filename = os.path.join(path_to_jsons, filename)
    obj_as_dict = obj.json
    with open(filename, 'w') as f:
        json.dump(obj_as_dict, f, indent=4)


def load_from_json(cls: Type[JsonSerializable], path_to_jsons: str = 'jsons') -> JsonSerializable:
    filename = '{}.{}.json'.format(cls.__module__, cls.__name__)
    filename = os.path.join(path_to_jsons, filename)
    with open(filename, 'r') as f:
        obj_as_dict = json.load(f)
    return cls.from_json(obj_as_dict)


def print_msg_if_allowed(msg, allow=False):
    if allow:
        print(msg)
