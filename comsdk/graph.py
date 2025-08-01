import collections
import os
import time
from enum import Enum, auto
from functools import partial
import importlib as imp

import comsdk.misc as aux

ImplicitParallelizationInfo = collections.namedtuple('ImplicitParallelizationInfo', ['array_keys_mapping', 'branches_number', 'branch_i'])


class Func:
    __slots__ = (
        'module',
        'func',
        'comment',
        'name'
    )

    def __init__(self, module="", name="", dummy=False, func=None, comment=''):
        self.module = module
        self.name = name
        self.comment = comment.replace("\0", " ") if comment is not None else ""
        if module == "" or name == "" or module is None or name is None:
            dummy = True
        if func is not None:
            self.func = func
        elif dummy:
            self.func = lambda data: data
        else: 
            print("LOADING function {} from {} module".format(name, module) )
            try:
                self.func = getattr(imp.import_module(module), name)
            except Exception:
                raise Exception("Could not load function {} from {} module".format(name, module))

    def __str__(self):
        if self.module == "" or self.name == "":
            return self.func.__name__
        return "{}_{}".format(self.module, self.name)


class Selector(Func):
    def __init__(self, ntransf, module="", name="", dummy=False):
        if module == "" and name == "":
            dummy = True
        self.dummy = dummy
        super().__init__(module, name, func=(lambda x: [True for i in range(ntransf)]) if dummy else None)

    def __str__(self):
        if self.module == "" or self.name == "":
            return ''
        return "{}_{}".format(self.module, self.name)


class Transfer:
    def __init__(self, edge, output_state, order=0):
        self.edge = edge
        self.output_state = output_state
        self.order = order

    def transfer(self, data, dynamic_keys_mapping={}):
        self.edge.morph(data, dynamic_keys_mapping)
        return self.output_state


class IdleRunType(Enum):
    INIT = auto()
    CLEANUP = auto()


class PluralState:
    def __init__(self, states):
        self.states = states
        pass

    def connect_to(self, term_states, edge):
        for init_state, term_state in zip(self.states, term_states):
            init_state.transfers.append(Transfer(edge, term_state))


class Graph:
    '''
    Class describing a graph-based computational method. Graph execution must start from this object.     
    '''
    def __init__(self, init_state,
                 term_state=None,
                 ):
        self.init_state = init_state
        self.term_state = term_state
        if self.term_state is not None:
            self.term_state.is_term_state = True
        self.current_state = None
        self.execution_path = []
        self.listeners = []
        self._initialized = False

    def __repr__(self):
        return (
            f"Graph(\n"
            f"  init_state={self.init_state!r},\n"
            f"  term_state={self.term_state!r},\n"
            f"  initialized={self._initialized}\n"
            f")"
        )

    def add_listener(self, listener):
        print("[DEBUG] listener added:", listener)
        self.listeners.append(listener)

    def _notify_listeners(self, event_type, state, data):
        print("[DEBUG] Notifying listeners:", len(self.listeners))
        for listener in self.listeners:
            listener({
                'event': event_type,
                'state': state.name if state else None,
                'timestamp': time.time(),
                'data': data.copy()
            })

    def run(self, data):
        '''
        Goes through the graph and returns boolean denoting whether the graph has finished successfully.
        It runs twice -- the first run is idle (needed for initialization) and the second run is real.
        The input data will be augmented by metadata:
        1) '__CURRENT_WORKING_DIR__' -- absolute path to the current working directory as defined by the OS
        2) '__WORKING_DIR__' -- absolute path to the directory from which external binaries or resources will be launched.
        It will be set only if it is not yet set in data
        3) '__EXCEPTION__' if any error occurs
        '''
        self.init_graph(data)
        cur_state = self.init_state
        implicit_parallelization_info = None
        while cur_state is not None:


            self.current_state = cur_state
            self._notify_listeners('state_enter', cur_state, data)
 #           print('1) In main loop', implicit_parallelization_info)
#            morph = _run_state(cur_state, data, implicit_parallelization_info)
            time.sleep(3)
            transfer_f, implicit_parallelization_info = _run_state(cur_state, data, implicit_parallelization_info)
#            print('2) In main loop', implicit_parallelization_info)
            if '__EXCEPTION__' in data:
                return False
#            cur_state, implicit_parallelization_info = morph(data)
            cur_state = transfer_f(data)
            print(cur_state)
#            print(morph)
            if '__EXCEPTION__' in data:
                return False
            self._notify_listeners('state_exit', cur_state, data)
            if cur_state:
                self.execution_path.append({
                    'state': cur_state.name,
                    'data': data.copy()
                })
        self._notify_listeners('complete', None, data)
        return True

    def init_graph(self, data={}):
        print("[DEBUG] entered init_graph()")
        if not self._initialized:
            print("[DEBUG] calling idle_run(INIT)")
            self.init_state.idle_run(IdleRunType.INIT, [self.init_state.name])
            self._initialized = True
        else:
            self.init_state.idle_run(IdleRunType.CLEANUP, [self.init_state.name])
        data['__CURRENT_WORKING_DIR__'] = os.getcwd()
        if not '__WORKING_DIR__' in data:
            data['__WORKING_DIR__'] = data['__CURRENT_WORKING_DIR__']


class State:
    __slots__ = [
        'name',
        'input_edges_number', #output_edges_number == len(transfers)
        'looped_edges_number',
        'activated_input_edges_number',
        'transfers',
        'parallelization_policy',
        'selector',
        'is_term_state',
        'array_keys_mapping',
        '_branching_states_history',
        '_proxy_state',
        'possible_branches',
        'comment'
        ]
    def __init__(self, name, 
                 parallelization_policy=None,
                 selector=None,
                 array_keys_mapping=None, # if array_keys_mapping is not None, we have implicit parallelization in this state
                 ):
        self.name = name
        self.parallelization_policy = SerialParallelizationPolicy() if parallelization_policy is None else parallelization_policy
        self.selector = Selector(1) if selector is None else selector
        self.array_keys_mapping = array_keys_mapping
        self.input_edges_number = 0
        self.looped_edges_number = 0
        self.activated_input_edges_number = 0
        self.transfers = []
        self.possible_branches = []
        self.is_term_state = False
        self._branching_states_history = None
        self._proxy_state = None
        self.comment = None

    def idle_run(self, idle_run_type, branching_states_history):
        def __sort_by_order(tr):
            return tr.edge.order
        self.transfers.sort(key = __sort_by_order)
        # print(self.name)
        # for t in self.transfers:
            # print("\t", t.edge.order, t.edge.pred_name, t.edge.morph_name)
        if self._proxy_state is not None:
            return self._proxy_state.idle_run(idle_run_type, branching_states_history)
        if idle_run_type == IdleRunType.INIT:
            self.input_edges_number += 1
            if self.input_edges_number != 1:
                if self._is_looped_branch(branching_states_history):
                    self.looped_edges_number += 1
                return # no need to go further if we already were there
            if self._branching_states_history is None:
                self._branching_states_history = branching_states_history
        elif idle_run_type == IdleRunType.CLEANUP:
            self.activated_input_edges_number = 0
            if self._branching_states_history is not None and self._is_looped_branch(branching_states_history):
                self._branching_states_history = None
                return
            if self._branching_states_history is None:
                self._branching_states_history = branching_states_history
        else:
            self.activated_input_edges_number += 1 # BUG: here we need to choose somehow whether we proceed or not
        # if len(self.transfers) == 0:
            # print('Terminate state found')
        if len(self.transfers) == 1:
            self.transfers[0].output_state.idle_run(idle_run_type, branching_states_history)
        else:
            for i, transfer in enumerate(self.transfers):
                next_state = transfer.output_state
                next_state.idle_run(idle_run_type, branching_states_history + [next_state.name])

    def connect_to(self, term_state, edge=None, comment=None):
        if comment is not None or comment != "":
            self.comment = comment
        self.transfers.append(Transfer(edge, term_state))
        self.selector = Selector(len(self.transfers))
#        edge.set_output_state(term_state)
#        self.output_edges.append(edge)

    def replace_with_graph(self, graph):
        self._proxy_state = graph.init_state
        graph.term_state.transfers = self.transfers
        graph.term_state.selector = self.selector

    def run(self, data, implicit_parallelization_info=None):
        print('STATE {}\n\tjust entered, implicit_parallelization_info: {}'.format(self.name, implicit_parallelization_info))
        # print('\t{}'.format(data))
        if self._proxy_state is not None:
            return self._proxy_state.run(data, implicit_parallelization_info)
        self._activate_input_edge(implicit_parallelization_info)
        #self.activated_input_edges_number += 1
        print('\trequired input: {}, active: {}, looped: {}'.format(self.input_edges_number, self.activated_input_edges_number, self.looped_edges_number))
#        print('qwer')
        if not self._ready_to_transfer(implicit_parallelization_info):
            return None, None # it means that this state waits for some incoming edges (it is a point of collision of several edges)
        self._reset_activity(implicit_parallelization_info)
        if self.is_term_state:
            implicit_parallelization_info = None
        if len(self.transfers) == 0:
            return transfer_to_termination, None
        dynamic_keys_mapping = build_dynamic_keys_mapping(implicit_parallelization_info)
        selected_edges = self.selector.func(data)
        if not selected_edges:
            raise GraphUnexpectedTermination(
                "STATE {}: error in selector: {} ".format(self.name, selected_edges))
#        selected_transfers = [self.transfers[i] for i, _ in enumerate(selected_edges) if selected_edges[i]]
#        for transf in selected_transfers:
#            if not transf.edge.predicate(data, dynamic_keys_mapping):
#                raise Exception("\tERROR: predicate {} returns {} running from state {}\n data{}".format(transf.edge.pred_f.name,transf.edge.predicate(data, dynamic_keys_mapping), self.name, data))
        selected_transfers = [self.transfers[i] for i, _ in enumerate(selected_edges)
                              if selected_edges[i] and self.transfers[i].edge.predicate(data, dynamic_keys_mapping)]
        if not selected_transfers:
            raise GraphUnexpectedTermination('\tERROR: no transfer function has been '
                                             'selected out of {} ones. Predicate values are {}. '
                                             'Selector values are {}.'.format(len(self.transfers),
                                                                              [t.edge.predicate(data, dynamic_keys_mapping) for t in self.transfers],
                                                                              selected_edges))
        return self.parallelization_policy.make_transfer_func(selected_transfers,
                                                              array_keys_mapping=self.array_keys_mapping,
                                                              implicit_parallelization_info=implicit_parallelization_info,
                                                              state=self), \
               implicit_parallelization_info

    def _activate_input_edge(self, implicit_parallelization_info=None):
        if implicit_parallelization_info is None or self.is_term_state:
            self.activated_input_edges_number += 1
        else:
            if isinstance(self.activated_input_edges_number, int):
                self.activated_input_edges_number = [0 for i in range(implicit_parallelization_info.branches_number)]
            self.activated_input_edges_number[implicit_parallelization_info.branch_i] += 1

    def _ready_to_transfer(self, implicit_parallelization_info=None):
        required_activated_input_edges_number = self.input_edges_number - self.looped_edges_number
        if implicit_parallelization_info is not None:
            if self.is_term_state:
                required_activated_input_edges_number = implicit_parallelization_info.branches_number
                return self.activated_input_edges_number == required_activated_input_edges_number
            return self.activated_input_edges_number[implicit_parallelization_info.branch_i] == required_activated_input_edges_number
        else:
            return self.activated_input_edges_number == required_activated_input_edges_number

#        if implicit_parallelization_info is None or self.is_term_state:
#            if self.is_term_state:
#                required_activated_input_edges_number = implicit_parallelization_info.branches_number
#            return self.activated_input_edges_number == required_activated_input_edges_number
#        else:
#            return self.activated_input_edges_number[implicit_parallelization_info.branch_i] == required_activated_input_edges_number

    def _reset_activity(self, implicit_parallelization_info=None):
        self._branching_states_history = None
        if self._ready_to_transfer(implicit_parallelization_info) and self._has_loop():
            if implicit_parallelization_info is None or self.is_term_state:
                self.activated_input_edges_number -= 1
            else:
                self.activated_input_edges_number[implicit_parallelization_info.branch_i] -= 1
        else:
#            self.activated_input_edges_number = 0
            if implicit_parallelization_info is None or self.is_term_state:
                self.activated_input_edges_number = 0
            else:
                self.activated_input_edges_number[implicit_parallelization_info.branch_i] = 0

    def _is_looped_branch(self, branching_states_history):
        return set(self._branching_states_history).issubset(branching_states_history)

    def _has_loop(self):
        return self.looped_edges_number != 0


def transfer_to_termination(data):
    return None

class SerialParallelizationPolicy:
#    def __init__(self, data):
#        self.data = data
    def __init__(self):
        pass

    def make_transfer_func(self, transfers, array_keys_mapping=None, implicit_parallelization_info=None, state=None):
        def _morph(data):
            # print("MORPHING FROM {}".format(state.name))
            if array_keys_mapping is None:
                dynamic_keys_mapping = build_dynamic_keys_mapping(implicit_parallelization_info)
                next_transfers = [partial(t.transfer, dynamic_keys_mapping=dynamic_keys_mapping) for t in transfers]
                next_impl_para_infos = [implicit_parallelization_info for _ in transfers]
 #               print('\t\t {}'.format(implicit_parallelization_infos))
            else:
                if len(transfers) != 1:
                    raise BadGraphStructure('Impossible to create implicit paralleilzation in the state '
                                            'with {} output edges'.format(len(transfers)))
                dynamic_keys_mapping = build_dynamic_keys_mapping(implicit_parallelization_info)
                proxy_data = aux.ProxyDict(data, keys_mappings=array_keys_mapping)
                anykey = next(iter(array_keys_mapping.keys()))
                implicit_branches_number = len(proxy_data[anykey])
                next_transfers = []
                next_impl_para_infos = []
                for branch_i in range(implicit_branches_number):
                    implicit_parallelization_info_ = ImplicitParallelizationInfo(array_keys_mapping, implicit_branches_number, branch_i)
                    dynamic_keys_mapping = build_dynamic_keys_mapping(implicit_parallelization_info_)
#                    print(dynamic_keys_mapping)
                    #next_transfers.append(partial(transfers[0].edge.morph, dynamic_keys_mapping=dynamic_keys_mapping))
                    next_transfers.append(partial(transfers[0].transfer, dynamic_keys_mapping=dynamic_keys_mapping))
                    next_impl_para_infos.append(implicit_parallelization_info_)
            cur_transfers = []
            cur_impl_para_infos = []
            #while len(next_transfers) != 1 or _is_implicitly_parallelized(next_impl_para_infos):
            while len(next_transfers) != 1 or _requires_joint_of_implicit_parallelization(array_keys_mapping, next_impl_para_infos):
                if next_impl_para_infos == []:
                    raise Exception("Morphs count on state {} is {}".format(state.name, str(len(next_transfers))))
#                print(array_keys_mapping, next_impl_para_infos)
                cur_transfers[:] = next_transfers[:]
                cur_impl_para_infos[:] = next_impl_para_infos[:]
                del next_transfers[:]
                del next_impl_para_infos[:]
                for t, impl_para_info in zip(cur_transfers, cur_impl_para_infos):
                    next_state = t(data)
#                    print('\t next_state: {}, with impl para info: {}'.format(next_state.name, impl_para_info))
                    if next_state is None:
                        return None
                    next_t, next_impl_para_info = _run_state(next_state, data, impl_para_info)
#                    print('\t next_morph: {}'.format(next_morph))
                    if '__EXCEPTION__' in data:
                        return None
                    if next_t is not None:
                        next_transfers.append(next_t)
                        next_impl_para_infos.append(next_impl_para_info)
#                print(array_keys_mapping, next_impl_para_infos)
                #print(len(next_transfers))
#            print('\t last morph: {}'.format(next_transfers[0]))
            next_state = next_transfers[0](data)
#            print(next_state.name, next_impl_para_infos[0])
            return next_state
        return _morph


class BadGraphStructure(Exception):
    pass


class GraphUnexpectedTermination(Exception):
    pass


def _requires_joint_of_implicit_parallelization(array_keys_mapping, impl_para_infos):
    if array_keys_mapping is None:
        return False
    for obj in impl_para_infos:
        if obj is not None:
            return True
    return False


def _get_trues(boolean_list):
    return [i for i, val in enumerate(boolean_list) if val == True]


def _run_state(state, data, implicit_parallelization_info=None):
    try:
        next_morphism, next_impl_para_info = state.run(data, implicit_parallelization_info)
    except GraphUnexpectedTermination as e:
        data['__EXCEPTION__'] = str(e)
        return None, None
    return next_morphism, next_impl_para_info


def build_dynamic_keys_mapping(implicit_parallelization_info=None):
    if implicit_parallelization_info is None:
        return {}
    dynamic_keys_mapping = {}
    for key, keys_path in implicit_parallelization_info.array_keys_mapping.items():
        dynamic_keys_mapping[key] = aux.ArrayItemGetter(keys_path, implicit_parallelization_info.branch_i)
    return dynamic_keys_mapping
