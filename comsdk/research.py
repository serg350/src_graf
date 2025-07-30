import pickle
from datetime import date
from typing import Sequence, Mapping, TypedDict

from comsdk.misc import *
from comsdk.communication import BaseCommunication, LocalCommunication, SshCommunication, Host
from comsdk.distributed_storage import *
from comsdk.edge import Func, Edge, dummy_predicate
from comsdk.graph import Graph, State

CopiesList = TypedDict('CopiesList', {'path': str, 'new_name': str})


class Research:

    """
    Class Research is a representation of a group of different calculations collected into what we call a Research.
    Each ''calculation'' corresponds to the launch of a graph-based scenario which produces a set of files which we
    treat as the results of the calculation. We thus call such a calculation a task. Therefore, a Research is a
    collection of tasks. Each task is associated with a single directory (within the code, it may be denoted as
    task_dir, if only directory name is of interest, or task_path, if the absolute path is of interest) whose name has
    a very simple structure, @number@-@long_name@, so that each task is associated with its own unique number (also
    called task_number within the code). Normally, one should use the task number to get any task-related information.
    All the tasks are located in the research directory whose the local (remote) absolute path is set by the class
    property local_research_path (remote_research_path). The research directory has the following pattern:
    @date@_@long_name@. Finally, we associate a short Research ID with each Research. The described structure is
    independent of where these directories are located. It is assumed that there is a local root for research and
    its remote analog. The latter should be available via any protocol supported by communication module. Class Research
    can thus be set up in two regimes: local (remote_comm is None) and local-remote (remote_comm is not None).

    Typically, one should construct an instance of Research based on the configuration file called config_research.json.
    There are two static functions for this purpose: Research.open() and Research.create(). The former creates an
    instance of Research based on the existing Research (one should pass its Research ID to open()) described in the
    configuration file and the latter creates a new Research (thus, making a new directory in the local filesystem) and
    adds all the necessary information about it in the configuration file. Also, any Research instance is automatically
    augmented by the properties listed in 'RESEARCH_PROPS' dictionary in the configuration file.

    For the Research constructor to understand where all the research directories are located, one must supply (either
    directly in the constructor or in the configuration file) the potential root paths for the search (both for the
    local and remote machines if the latter is specified). The first path in the list of the potential root paths is
    called the default root path. A new Research will be created in the default path.

    Note that different tasks belonging to the same research (i.e., they are associated with the same Research ID) may
    be located at different root paths. When creating a new task, it will be located in the default root path.

    .. todo::
        Some way for saving auxiliary information about research and tasks (task date and description, for example)
        should be implemented. Possibly, the same should be done for launcher scripts.

    """
    def __init__(self, name: str,
                 continuing=False,
                 local_research_roots: Optional[Sequence[str]] = None,
                 remote_comm: Optional[BaseCommunication] = None,
                 remote_research_root: Optional[str] = None,
                 personal_task_shift=0):
        """
        :param name: research description (if continuing == False) or research directory (if continuing == True)
        :param continuing: if False, the Research with be read from the root path. Otherwise, a new one will be created
        :param local_research_roots: a list of local paths where research directories are searched for
        :param remote_comm: BaseCommunication instance used for communication with remote machine
        :param remote_research_root: path on the remote machine where research directories are searched for
        """
        self._local_research_root = local_research_roots[0]
        self._local_root = os.path.dirname(self._local_research_root)
        self._remote_research_root = remote_research_root
        self._personal_task_shift = personal_task_shift
        self._tasks_number = personal_task_shift
        self._local_comm = LocalCommunication(Host())  # local communication created automatically, no need to pass it
        self._remote_comm = remote_comm
        self._distr_storage = DistributedStorage(local_research_roots, prior_storage_index=0)
        self._local_research_path = None
        if not continuing:
            # interpret name as name without date
            self._research_dir = make_suitable_research_dir(name)
            if self._distr_storage.get_dir_path(self._research_dir) is not None:
                raise ResearchAlreadyExists("Research with name '{}' already exists, "
                                            "choose another name".format(self._research_dir))
            self._local_research_path = self._distr_storage.make_dir(self._research_dir)
            print('Started new research at {}'.format(self._local_research_path))
        else:
            # interpret name as the full research id
            self._research_dir = name
            self._local_research_path = self._load_research_data()

    @classmethod
    def open(cls, research_id: str,
             remote_comm: Optional[BaseCommunication] = None):
        """
        :param research_id: Research ID used to find a relevant research
        :param remote_comm: BaseCommunication instance used for communication with remote machine
        :return: new Research instance
        """
        with open(os.path.expanduser('~/.comsdk/config_research.json'), 'r') as f:
            conf = json.load(f)
        res = Research(conf['RESEARCH'][research_id],
                       continuing=True,
                       local_research_roots=conf['LOCAL_HOST']['research_roots'],
                       remote_comm=remote_comm,
                       remote_research_root=conf['REMOTE_HOSTS'][remote_comm.machine_name]['research_root']
                                            if remote_comm is not None else None,
                       personal_task_shift=conf['PERSONAL_TASK_SHIFT'])
        res._add_properties(conf['RESEARCH_PROPS'])
        return res

    @classmethod
    def create(cls, new_research_id: str, new_research_descr: str,
               remote_comm: Optional[BaseCommunication] = None):
        """
        :param new_research_id: Research ID (short name for this research)
        :param new_research_descr: relatively long research name
        :param remote_comm: BaseCommunication instance used for communication with remote machine
        :return: new Research instance
        """
        with open(os.path.expanduser('~/.comsdk/config_research.json'), 'r+') as f:
            conf = json.load(f)
            conf['RESEARCH'][new_research_id] = make_suitable_research_dir(new_research_descr)
            f.seek(0)
            json.dump(conf, f, indent=4)
            f.truncate()
        res = Research(new_research_descr,
                       continuing=False,
                       local_research_roots=conf['LOCAL_HOST']['research_roots'],
                       remote_comm=remote_comm,
                       remote_research_root=conf['REMOTE_HOSTS'][remote_comm.machine_name]['research_root']
                                            if remote_comm is not None else None,
                       personal_task_shift=conf['PERSONAL_TASK_SHIFT'])
        res._add_properties(conf['RESEARCH_PROPS'])
        return res

    @property
    def local_research_path(self) -> str:
        return self._local_research_path

    @property
    def remote_research_path(self) -> str:
        return os.path.join(self._remote_research_root, self._research_dir)

    @property
    def local_root(self) -> str:
        return self._local_root

    @property
    def research_dir(self) -> str:
        return self._research_dir

    def __getstate__(self) -> dict:
        return {
            'research_dir': self._research_dir,
            'local_research_path': self._local_research_root,
            'remote_research_path': self._remote_research_root,
            'personal_task_shift': self._personal_task_shift,
            'remote_comm': self._remote_comm.__getstate__(),
        }

    def __setstate__(self, state):
        self._personal_task_shift = state['personal_task_shift']
        self._tasks_number = self._personal_task_shift
        self._local_comm = LocalCommunication(Host())
        self._local_research_root = state['local_research_path']
        self._remote_research_root = state['remote_research_path']
        self._remote_comm = None
        if state['remote_comm'] is not None:
            self._remote_comm = SshCommunication.__new__(SshCommunication)
            self._remote_comm.__setstate__(state['remote_comm'])
        self._distr_storage = DistributedStorage((self._local_research_root,), prior_storage_index=0)
        self._research_dir = state['research_dir']
        self._research_path = self._load_research_data()

    def _add_properties(self, props: Mapping[str, Any]) -> None:
        for prop_name, prop_value in props.items():
            self.__setattr__(prop_name, prop_value)

    def _load_research_data(self) -> str:
        # find corresponding date/name
        # construct object from all data inside
        research_path = self._distr_storage.get_dir_path(self._research_dir)
        if research_path is None:
            raise ResearchDoesNotExist("Research '{}' does not exist".format(self._research_dir))

        print('Loaded research at {}'.format(research_path))

        # determine maximum task number to set the number for the next possible task
        dirnames, _ = self._distr_storage.listdir(self._research_dir)
        for dir_ in dirnames:
            if dir_ != 'report':
                task_number, _ = split_task_dir(dir_)
                if task_number > self._tasks_number:
                    self._tasks_number = task_number
        self._tasks_number += 1
        print('Next created task in the current research will hold the following number: {}'.format(self._tasks_number))
        return research_path

    def create_task(self, name: str) -> int:
        """
        Creates a new task in the current research making a new local directory

        :param name: task name
        :return: task number
        """
        task_number = self._get_next_task_number()
        local_task_dir = self._make_task_path(task_number, name)
        os.mkdir(local_task_dir)
        return task_number

    def grab_task_results(self, task_number: int,
                          copies_list: Optional[Sequence[CopiesList]] = None):
        """
        Moves task content from the remote machine to the local one. Locally, the task content will appear in the task
        directory located in the research directory.

        :param task_number: task number
        :param copies_list: a list defining which objects we wish to copy from the remote machine. It consists of
        dictionaries each having keys 'path' (path of object we wish to copy relative to the task directory) and
        'new_name' (path of this object on the local machine relative to the task directory)
        """
        task_results_local_path = self.get_task_path(task_number)
        task_results_remote_path = self.get_task_path(task_number, self._remote_comm.host)
        if copies_list is None: # copy all data
            paths = self._remote_comm.listdir(task_results_remote_path)
            for file_or_dir in paths:
                self._remote_comm.copy('/'.join((task_results_remote_path, file_or_dir)), task_results_local_path,
                                       'from_remote', show_msg=True)
        else:
            for copy_target in copies_list:
                # we consider copy targets as relative to task's dir
                remote_copy_target_path = '/'.join((task_results_remote_path, copy_target['path']))
                self._remote_comm.copy(remote_copy_target_path, task_results_local_path, 'from_remote', show_msg=True)
                if 'new_name' in copy_target:
                    os.rename(os.path.join(task_results_local_path, os.path.basename(copy_target['path'])),
                              os.path.join(task_results_local_path, copy_target['new_name']))

    def _make_task_path(self, task_number: int, task_name: str, at_remote_host=False) -> str:
        task_path = None
        task_dir = get_task_full_name(task_number, task_name)
        if at_remote_host:
            task_path = os.path.join(self._remote_research_root, self._research_dir, task_dir)
        else:
            task_path = os.path.join(self._local_research_path, task_dir)
        return task_path

    def get_task_path(self, task_number: int, at_remote_host=False) -> str:
        """
        Return absolute task path based on its number

        :param task_number: task number
        :param at_remote_host: return the path on the remote machine (if True) or on the local one (if False)
        :return: absolute task path
        """
        task_path = None
        task_name = self._get_task_name_by_number(task_number)
        rel_task_dir = os.path.join(self._research_dir, get_task_full_name(task_number, task_name))
        if at_remote_host:
            if self._remote_comm is None:
                raise ValueError('Cannot get a task path on the remote: remote communication is not set up')
            task_path = '{}/{}'.format(self._remote_research_root, rel_task_dir)
        else:
            task_path = self._distr_storage.get_dir_path(rel_task_dir)
        return task_path

    def dump_object(self, task_number: int, obj: object, obj_name: str) -> None:
        """
        Dumps any python object (using pickle) to the binary file, named obj_name + '.pyo', in the task directory
        associated with the task number

        :param task_number: task number
        :param obj: any python object
        :param obj_name: file name to which obj will be saved (without extension)
        """

        print('Dumping ' + obj_name)
        f = open(os.path.join(self.get_task_path(task_number), obj_name + '.pyo'), 'w')
        pickle.dump(obj, f)
        f.close()

    def load_object(self, task_number: int, obj_name: str):
        """
        Load any python object dumped using pickle from the binary file, named obj_name + '.pyo' and located in the task
        directory associated with the task number

        :param task_number: task number
        :param obj_name: file name from which obj will be loaded (without extension)
        :return: python object
        """
        print('Loading ' + obj_name)
        f = open(os.path.join(self.get_task_path(task_number), obj_name + '.pyo'), 'r')
        obj = pickle.load(f)
        f.close()
        return obj

    def _get_next_task_number(self) -> int:
        self._tasks_number += 1
        return self._tasks_number - 1

    def _get_task_name_by_number(self, task_number: int) -> str:
        find_data = self._distr_storage.find_dir_by_named_regexp(self._research_dir,
                                                                 '^{}-(?P<task_name>\S+)'.format(task_number))
        if find_data is None:
            raise Exception("No task with number '{}' is found".format(task_number))
        return find_data[1]['task_name']


class ResearchAlreadyExists(Exception):
    pass


class ResearchDoesNotExist(Exception):
    pass


def make_suitable_name(name: str) -> str:
    return '-'.join(name.split())


def make_suitable_task_name(name: str) -> str:
    return '_'.join(name.split())


def make_suitable_research_dir(descr: str) -> str:
    return '-'.join([str(date.today()), make_suitable_name(descr)])


def get_task_full_name(task_number: int, task_name: str) -> str:
    return str(task_number) + '-' + make_suitable_task_name(task_name)


def split_task_dir(task_dir: str) -> (int, str):
    parsing_params = parse_by_named_regexp(r'^(?P<task_number>\d+)-(?P<task_name>\S+)', task_dir)
    if parsing_params is None:
        raise Exception("No task directory '{}' is found".format(task_dir))
    return int(parsing_params['task_number']), parsing_params['task_name']


def retrieve_trailing_float_from_task_dir(task_dir: str) -> float:
    matching = re.search(r'^(?P<task_number>\d+)-(?P<task_name>\S+)_(?P<float_left>\d+)\.(?P<float_right>\d+)',
                         task_dir)
    if matching is None:
        raise Exception('Incorrect task directory is given')
    return float('{}.{}'.format(matching.group('float_left'), matching.group('float_right')))


class CreateTaskEdge(Edge):
    def __init__(self, res, task_name_maker, predicate=dummy_predicate, remote=False):
        self._res = res
        self._task_name_maker = task_name_maker
        self._remote = remote
        super().__init__(predicate, Func(func=self.execute))

    def execute(self, data):
        task_name = self._task_name_maker(data)
        task_number = self._res.create_task(task_name)
        data['__WORKING_DIR__'] = self._res.get_task_path(task_number)
        if self._remote:
            data['__REMOTE_WORKING_DIR__'] = self._res.get_task_path(task_number, at_remote_host=True)


class CreateTaskGraph(Graph):
    def __init__(self, res, task_name_maker, array_keys_mapping=None, remote=False):
        s_init, s_term = self.create_branch(res, task_name_maker, array_keys_mapping=array_keys_mapping, remote=remote)
        super().__init__(s_init, s_term)

    @staticmethod
    def create_branch(res, task_name_maker, array_keys_mapping=None, remote=False):
        s_init = State('READY_FOR_TASK_CREATION', array_keys_mapping=array_keys_mapping)
        s_term = State('TASK_CREATED')
        s_init.connect_to(s_term, edge=CreateTaskEdge(res, task_name_maker=task_name_maker, remote=remote))
        return s_init, s_term
