import os
import posixpath
import pickle
from typing import Sequence, Tuple, Optional
import logging
import json

from mako.template import Template

import comsdk.misc as aux
from comsdk.communication import CommunicationError
from comsdk.graph import Func, State


dummy_predicate = Func(func=lambda d: True)
dummy_morphism = Func()
job_finished_predicate = Func(func=lambda d: d['job_finished'])
job_unfinished_predicate = Func(func=lambda d: not d['job_finished'])


class InOutMapping(object):
    def __init__(self,
                 keys_mapping={},
                 relative_keys=(),
                 default_relative_key=(),
                ):
        self._default_relative_key = default_relative_key if aux.is_sequence(default_relative_key) else (default_relative_key,)
        self._relative_keys = relative_keys if aux.is_sequence(relative_keys) else (relative_keys,)
        self._keys_mapping = keys_mapping

    def __str__(self):
        return 'Default relative key: {}\n' \
               'Relative keys:\n{}\n' \
               'Keys mapping:\n\tLocal -> Global\n\t----------------\n' \
               '{}'.format('.'.join(self._default_relative_key),
                           '\n'.join(['\t' + '.'.join(k) for k in self._relative_keys]),
                           '\n'.join(['\t' + loc + ' -> ' + '.'.join(glo) for loc, glo in self._keys_mapping]))

    def build_proxy_data(self, data, dynamic_keys_mapping={}):
        if self._default_relative_key == () and self._relative_keys == () and self._keys_mapping == {} and dynamic_keys_mapping == {}:
            return data
        else:
            #print('\t{}\n\t{}\n\t{}'.format(self._relative_keys, self._keys_mapping, dynamic_keys_mapping))
            return aux.ProxyDict(data, self._relative_keys, dict(self._keys_mapping, **dynamic_keys_mapping), self._default_relative_key)


class Edge:
    __slots__ = [
        'pred_f',
        'morph_f',
        '_io_mapping',
        'preprocess',
        'postprocess',
        'order',
        'comment',
        'mandatory_keys',
        'use_proxy_data_for_pre_post_processing'
    ]

    def __init__(self, predicate, morphism, 
                 io_mapping=InOutMapping(),
                 order=0, 
                 comment="",
                 mandatory_keys=(),
                 ):
        self.pred_f = predicate
        self.morph_f = morphism
        self._io_mapping = io_mapping
        self.preprocess = lambda pd: None
        self.postprocess = lambda pd: None
        self.order = int(0 if order is None else order)
        self.comment = comment
        self.mandatory_keys = mandatory_keys
        self.use_proxy_data_for_pre_post_processing=False

    def predicate(self, data, dynamic_keys_mapping={}):
        proxy_data = self._io_mapping.build_proxy_data(data, dynamic_keys_mapping)
        return self.pred_f.func(proxy_data)

    def morph(self, data, dynamic_keys_mapping={}):
        #print(self.pred_name, self.morph_name, self.order)
        proxy_data = self._io_mapping.build_proxy_data(data, dynamic_keys_mapping)
        # print(proxy_data)
        if (self.use_proxy_data_for_pre_post_processing):
            self.preprocess(proxy_data)
        else:
            self.preprocess(data)
        self._throw_if_not_set(proxy_data, self.mandatory_keys)
        self.morph_f.func(proxy_data)
        if (self.use_proxy_data_for_pre_post_processing):
            self.postprocess(proxy_data)
        else:
            self.postprocess(data)

    def _throw_if_not_set(self, data, mandatory_keys: Sequence[str]):
        for k in mandatory_keys:
            if k not in data:
                logging.exception('EDGE {}: key "{}" is not set whilst being mandatory.\nIOMapping:\n'
                                  '{}'.format(type(self).__name__, k, str(self._io_mapping)))
                raise KeyError()
#                raise KeyError('EDGE {}: key "{}" is not set whilst being mandatory.\nIOMapping:\n'
#                               '{}'.format(type(self).__name__, k, str(self._io_mapping)))

class ExecutableProgramEdge(Edge):
    '''
    Class implementing the edge which uses an external program to morph data.
    The program is lauchned via so-called communication which, among others, sets where the program is located and it can be launched.
    Environment can be used to launch program on remote resources.
    # DESCRIPTION OF KEYS MAPPINGS #
    Since data structure is hierarchical, we introduced keys mappings. The edge needs to use some variables
    from data which may be located in different (nested) keys of data (we will call these keys "global"). 
    However, it is very convenient to implement the edge imagining that there is no nested structures 
    and all keys are available in the top-level of data (we will call these keys "local").
    To link global and local keys, we introduce keys mapping, which are either dictionaries (local key string -> sequence) or sequences.
    If the keys mapping is sequence, we treat it as a relative "path" to all needed keys.
    Therefore, we have keys mappings for input and output keys.
    # END OF DESCRIPTION OF KEYS MAPPINGS #
    We expect that necessary input files are already on remote.
    Programs may require three types of arguments:
    1) keyword arguments (-somearg something)
    2) flags (-someflag)
    3) trailing arguments
    Local keys determining the corresponding values are located in keyword_names, flag_names and trailing_args_keys.
    Finally, data must be somehow updated after finishing. This will be done by updating data according to output_dict (it is just added)  
    '''
    def __init__(self, program_name, comm,
                 predicate=dummy_predicate,
                 io_mapping=InOutMapping(),
                 output_dict={},  # output dict which will be added to the main dictionary (w.r.t. output_keys_mapping)
                 keyword_names=(),  # "local keys" where keyword args are stored
                 flag_names=(),  # "local keys" where flags are stored
                 trailing_args_keys=(),  # "local keys" where trailing args are stored
                 remote=False,
                 stdout_processor=None,
                 chaining_command_at_start=lambda d: '',
                 chaining_command_at_end=lambda d: '',
                 ):
        #predicate = predicate if predicate is not None else dummy_predicate
        self._output_dict = output_dict
        self._comm = comm
        self._program_name = program_name
        self._keyword_names = keyword_names
        self._flag_names = flag_names
        self._trailing_args_keys = trailing_args_keys
        self._working_dir_key = '__REMOTE_WORKING_DIR__' if remote else '__WORKING_DIR__'
        mandatory_keys = [self._working_dir_key]
        self._stdout_processor = stdout_processor
        self.chaining_command_at_start = chaining_command_at_start
        self.chaining_command_at_end = chaining_command_at_end
        super().__init__(predicate, Func(func=self.execute), io_mapping, mandatory_keys=mandatory_keys)

    def execute(self, data):
        args_str = build_args_line(data, self._keyword_names, self._flag_names, self._trailing_args_keys)
        working_dir = data[self._working_dir_key]
        stdout_lines, stderr_lines = self._comm.execute_program(self._program_name, args_str, working_dir,
                                                                self.chaining_command_at_start(data),
                                                                self.chaining_command_at_end(data))
        output_data = self._output_dict
        if self._stdout_processor:
            stdout_data = self._stdout_processor(data, stdout_lines)
            data.update(stdout_data)
        data.update(output_data)


class QsubScriptEdge(Edge):
    '''
    Class implementing the edge which builds up the sh-script for qsub.
    The script is created via communication.
    # DESCRIPTION OF KEYS MAPPINGS #
    Since data structure is hierarchical, we introduced keys mappings. The edge needs to use some variables
    from data which may be located in different (nested) keys of data (we will call these keys "global"). 
    However, it is very convenient to implement the edge imagining that there is no nested structures 
    and all keys are available in the top-level of data (we will call these keys "local").
    To link global and local keys, we introduce keys mapping, which are either dictionaries (local key string -> sequence) or sequences.
    If the keys mapping is sequence, we treat it as a relative "path" to all needed keys.
    Therefore, we have keys mappings for input and output keys.
    # END OF DESCRIPTION OF KEYS MAPPINGS #
    Data will be augmented by 'qsub_script' pointing to the local file.
    '''
    def __init__(self, program_name, local_comm, remote_comm,
                 predicate=dummy_predicate,
                 io_mapping=InOutMapping(),
                 keyword_names=(),  # "local keys" where keyword args are stored
                 flag_names=(),  # "local keys" where flags are stored
                 trailing_args_keys=(),  # "local keys" where trailing args are stored
                 ):
#        predicate = predicate if predicate is not None else dummy_predicate
        self._local_comm = local_comm
        self._remote_comm = remote_comm
        self._program_name = program_name
        self._keyword_names = keyword_names
        self._flag_names = flag_names
        self._trailing_args_keys = trailing_args_keys
        mandatory_keys = ['__WORKING_DIR__', 'qsub_script_name', 'time_required', 'cores_required']
        super().__init__(predicate, Func(func=self.execute), io_mapping, mandatory_keys=mandatory_keys)

    def execute(self, data):
        if isinstance(data, aux.ProxyDict):
            print('QsubScriptEdge -> {}: {}'.format('qsub_script_name', data._keys_mappings['qsub_script_name']))
        qsub_script_path = os.path.join(data['__WORKING_DIR__'], data['qsub_script_name'])
        args_str = build_args_line(data, self._keyword_names, self._flag_names, self._trailing_args_keys)
        program_launch_path = self._remote_comm.host.get_program_launch_path(self._program_name)
        command_line = '{} {}'.format(program_launch_path, args_str)
        render_sge_template(self._remote_comm.host.sge_template_name, qsub_script_path, 
                            data['cores_required'], data['time_required'], (command_line,))
        data.update({'qsub_script': qsub_script_path})


class UploadOnRemoteEdge(Edge):
    '''
    Class implementing the edge which uploads the data to the remote computer.
    It is done via environment which must provide the interface for that.
    # DESCRIPTION OF KEYS MAPPINGS #
    Since data structure is hierarchical, we introduced keys mappings. The edge needs to use some variables
    from data which may be located in different (nested) keys of data (we will call these keys "global"). 
    However, it is very convenient to implement the edge imagining that there is no nested structures 
    and all keys are available in the top-level of data (we will call these keys "local").
    To link global and local keys, we introduce keys mapping, which are either dictionaries (local key string -> sequence) or sequences.
    If the keys mapping is sequence, we treat it as a relative "path" to all needed keys.
    Therefore, we have keys mappings for input and output keys.
    # END OF DESCRIPTION OF KEYS MAPPINGS #
    Files for uploading must be found in input_files_keys which is a list of local data keys corresponding to these files.
    They will be uploaded in remote working dir which must be in data['__REMOTE_WORKING_DIR__'].
    After edge execution, data is going to be updated such that local paths will be replaced by remote ones.
    '''
    def __init__(self, comm,
                 predicate=dummy_predicate,
                 io_mapping=InOutMapping(),
                 local_paths_keys=(),  # "local keys", needed to build a copy list
                 update_paths=True,
                 already_remote_path_key=None,
                 ):
#        predicate = predicate if predicate is not None else dummy_predicate
        self._local_paths_keys = local_paths_keys
        self._comm = comm
        self._update_paths = update_paths
        self._already_remote_path_key = already_remote_path_key
        mandatory_keys = list(self._local_paths_keys) + ['__WORKING_DIR__', '__REMOTE_WORKING_DIR__']
        if self._already_remote_path_key is not None:
            mandatory_keys.append(self._already_remote_path_key)
        super().__init__(predicate, Func(func=self.execute), io_mapping, mandatory_keys=mandatory_keys)

    def execute(self, data):
        if self._already_remote_path_key is not None:
            if data[self._already_remote_path_key]:
                return
        remote_working_dir = data['__REMOTE_WORKING_DIR__']
        for key in self._local_paths_keys:
            try:
                # try data[key] as an absolute path
                data[key] = self._comm.copy(data[key], remote_working_dir, mode='from_local')
            except CommunicationError as e:
                # try data[key] as a relative path
                working_dir = data['__WORKING_DIR__']
                if isinstance(data, aux.ProxyDict):
                    print('UploadOnRemoteEdge -> {}: {}'.format(key, data._keys_mappings[key]))
                remote_path = self._comm.copy(os.path.join(working_dir, data[key]), remote_working_dir,
                                              mode='from_local')
                if self._update_paths:
                    data[key] = remote_path


class DownloadFromRemoteEdge(Edge):
    '''
    Class implementing the edge which downloads the data from the remote computer.
    It is done via environment which must provide the interface for that.
    # DESCRIPTION OF KEYS MAPPINGS #
    Since data structure is hierarchical, we introduced keys mappings. The edge needs to use some variables
    from data which may be located in different (nested) keys of data (we will call these keys "global"). 
    However, it is very convenient to implement the edge imagining that there is no nested structures 
    and all keys are available in the top-level of data (we will call these keys "local").
    To link global and local keys, we introduce keys mapping, which are either dictionaries (local key string -> sequence) or sequences.
    If the keys mapping is sequence, we treat it as a relative "path" to all needed keys.
    Therefore, we have keys mappings for input and output keys.
    # END OF DESCRIPTION OF KEYS MAPPINGS #
    Files for downloading must be found in output_files_keys which is a list of local data keys corresponding to these files.
    All these files are relative to the remote working dir and will be downloaded into local working dir
    Local working dir must be in data['__LOCAL_WORKING_DIR__'].
    Remote working dir must be in data['__REMOTE_WORKING_DIR__'].
    After edge execution, data is going to be updated such that remote/relative paths will be replaced by local ones.
    '''
    def __init__(self, comm,
                 predicate=dummy_predicate,
                 io_mapping=InOutMapping(),
                 remote_paths_keys=(),  # "local keys", needed to build a list for downloading
                 update_paths=True,
                 show_msg=False,
                 ):
#        predicate = predicate if predicate is not None else dummy_predicate
        self._remote_paths_keys = remote_paths_keys
        self._comm = comm
        self._update_paths = update_paths
        self._show_msg = show_msg
        mandatory_keys = list(self._remote_paths_keys) + ['__WORKING_DIR__', '__REMOTE_WORKING_DIR__']
        super().__init__(predicate, Func(func=self.execute), io_mapping, mandatory_keys=mandatory_keys)

    def execute(self, data):
        working_dir = data['__WORKING_DIR__']
        remote_working_dir = data['__REMOTE_WORKING_DIR__']
        for key in self._remote_paths_keys:
            output_file_or_dir = data[key]
            if output_file_or_dir is None:
                continue
            local_path = None
            if output_file_or_dir == '*':
                aux.print_msg_if_allowed('\tAll possible output files will be downloaded', allow=self._show_msg)
                paths = self._comm.listdir(remote_working_dir)
                local_full_paths = ['/'.join([working_dir, file_or_dir]) for file_or_dir in paths]
                remote_full_paths = ['/'.join([remote_working_dir, file_or_dir]) for file_or_dir in paths]
                for file_or_dir in remote_full_paths:
                    aux.print_msg_if_allowed('\tAm going to download "{}" to "{}"'.format(file_or_dir, working_dir),
                                             allow=self._show_msg)
                    self._comm.copy(file_or_dir, working_dir, mode='from_remote', show_msg=self._show_msg)
                local_path = local_full_paths
            else:
                output_file_or_dir_as_list = []
                if isinstance(output_file_or_dir, list):
                    output_file_or_dir_as_list = output_file_or_dir
                else:
                    output_file_or_dir_as_list = [output_file_or_dir]
                for f in output_file_or_dir_as_list:
                    file_or_dir = '/'.join([remote_working_dir, f])
                    aux.print_msg_if_allowed('\tAm going to download "{}" to "{}"'.format(file_or_dir, working_dir),
                                             allow=self._show_msg)
                    local_path = self._comm.copy(file_or_dir, working_dir,
                                                 mode='from_remote', show_msg=self._show_msg)
            if self._update_paths:
                data[key] = local_path


def make_cd(key_path):
    def _cd(d):
        if key_path == '..':
            d['__WORKING_DIR__'] = os.path.dirname(d['__WORKING_DIR__'])
            if '__REMOTE_WORKING_DIR__' in d:
                d['__REMOTE_WORKING_DIR__'] = posixpath.dirname(d['__REMOTE_WORKING_DIR__'])
        else:
            subdir = aux.recursive_get(d, key_path)
            d['__WORKING_DIR__'] = os.path.join(d['__WORKING_DIR__'], subdir)
            if '__REMOTE_WORKING_DIR__' in d:
                d['__REMOTE_WORKING_DIR__'] = posixpath.join(d['__REMOTE_WORKING_DIR__'], subdir)
    return _cd


def make_mkdir(key_path, remote_comm=None):
    def _mkdir(d):
        remote = '__REMOTE_WORKING_DIR__' in d
        dir = os.path.join(d['__WORKING_DIR__'],
                               d[key_path])
        os.mkdir(dir)
        if '__REMOTE_WORKING_DIR__' in d:
            dir = os.path.join(d['__REMOTE_WORKING_DIR__'],
                               d[key_path])
            remote_comm._mkdirp(dir)
    return _mkdir


def make_dump(dump_name_format, format_keys=(), omit=None, method='pickle'):
    def _dump(d):
        format_params = [aux.recursive_get(d, key) for key in format_keys]
        dump_path = os.path.join(d['__WORKING_DIR__'], dump_name_format.format(*format_params))
        if omit is None:
            dumped_d = d
        else:
            if (isinstance(d, aux.ProxyDict)):
                dumped_d = {key: val for key, val in d._data.items() if not key in omit}
            else:
                dumped_d = {key: val for key, val in d.items() if not key in omit}
        if method == 'pickle':
            with open(dump_path, 'wb') as f:
                pickle.dump(dumped_d, f)
        elif method == 'json':
            with open(dump_path, 'w') as f:
                json.dump(dumped_d, f)
        else:
            raise ValueError(f'Method "{method}" is not supported in dumping')
    return _dump


def make_composite_func(*funcs):
    def _composite(d):
        res = None
        for func in funcs:
            f_res = func(d)
            # this trick allows us to combine returning
            # and non-returning functions
            if f_res is not None:
                res = f_res
        return res
    return _composite


def make_composite_predicate(*preds):
    def _composite(d):
        for pred in preds:
            if not pred(d):
                return False
        return True
    return _composite


def create_local_data_from_global_data(global_data, keys_mapping):
    if keys_mapping is None:
        return global_data
    elif aux.is_sequence(keys_mapping):
        return aux.recursive_get(global_data, keys_mapping)
    else:    
        return {local_key: aux.recursive_get(global_data, global_key) for local_key, global_key in keys_mapping.items()}


def update_global_data_according_to_local_data(local_data, global_data, keys_mapping):
    if keys_mapping is None:
        global_data.update(local_data)
    elif aux.is_sequence(keys_mapping):
        relative_data = aux.recursive_get(global_data, keys_mapping)
        relative_data.update(local_data)
    else:
        for local_key, global_key in keys_mapping.items():
            recursive_set(global_data, global_key, local_data[local_key])


def build_args_line(data, keyword_names, flag_names, trailing_args_keys):
    args_str = ''
    for keyword in keyword_names:
        if keyword in data:
            args_str += '-{} {} '.format(keyword, data[keyword])
    for flag in flag_names:
        if flag in data and data[flag]:
            args_str += '-{} '.format(flag)
    for place_i, trailing_arg_key in enumerate(trailing_args_keys):
        # if we have a sequence under the key, we expand it
        if trailing_arg_key in data:
            trailing_arg = data[trailing_arg_key]
            args_str += ' '.join(map(str, trailing_arg)) if aux.is_sequence(trailing_arg) else trailing_arg
            args_str += ' '
    return args_str


def render_sge_template(sge_template_name, sge_script_path, cores, time, commands):
    with open(os.path.expanduser('~/.comsdk/config_research.json'), 'r') as f:
        conf = json.load(f)
    sge_templ_path = os.path.join(conf['TEMPLATES_PATH'], sge_template_name)
    if not os.path.exists(sge_templ_path): # by default, templates are in templates/, but here we let the user put any path
        sge_templ_path = sge_template_name
    f = open(sge_templ_path, 'r')
    rendered_data = Template(f.read()).render(cores=cores, time=time, commands=commands)
    sge_script_file = aux.create_file_mkdir(sge_script_path)
    sge_script_file.write(rendered_data)


def connect_branches(branches: Sequence[Tuple[State, State]], edges: Optional[Sequence[Edge]] = None):
    if edges is None:
        edges = [dummy_edge for _ in range(len(branches) - 1)]
    for i, edge in zip(range(1, len(branches)), edges):
        _, prev_branch_end = branches[i - 1]
        next_branch_start, _ = branches[i]
        prev_branch_end.connect_to(next_branch_start, edge=edge)


dummy_edge = Edge(dummy_predicate, Func())
