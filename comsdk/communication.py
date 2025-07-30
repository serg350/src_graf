import os
import os.path
import shutil
import subprocess
import shlex
import json
import socket
from stat import S_ISDIR
from abc import ABCMeta, abstractmethod
import logging

import paramiko

import comsdk.misc as aux


class Host(object):
    '''
    Class storing all necessary information about the host of execution.
    '''
    def __init__(self):
        self.programs = {}
        self.commands = {}

    def add_program(self, prog_name,
                    path_to_prog=None,
                    ):
        self.programs[prog_name] = path_to_prog

    def add_command(self, cmd_name, cmd):
        self.commands[cmd_name] = cmd

    def get_program_launch_path(self, prog_name):
        if prog_name not in self.programs:
            raise ValueError(f'Program "{prog_name}" is not recognized. '
                             'Please add this program to "custom_programs" '
                             'in the corresponding host in the config file '
                             'if you want to use it.')
        path_to_prog = self.programs[prog_name]
        if path_to_prog is not None:
            print(self.programs[prog_name], prog_name)
            return self.join_path(self.programs[prog_name], prog_name)
        else:
            return prog_name

    def join_path(self, *path_list):
        return os.path.join(*path_list)


class RemoteHost(Host):
    '''
    RemoteHost extends Host including information about ssh host and the number of cores.
    '''
    def __init__(self, ssh_host, cores, sge_template_name, job_setter, job_finished_checker):
        self.ssh_host = ssh_host
        self.cores = cores
        self.sge_template_name = sge_template_name
        self.set_job_id = aux.load_function_from_module(job_setter)
        self.check_task_finished = aux.load_function_from_module(job_finished_checker)
        self._job_setter = job_setter
        self._job_finished_checker = job_finished_checker
        super().__init__()

    def __getstate__(self):
        return {
            'ssh_host': self.ssh_host,
            'cores': self.cores,
            'programs': self.programs,
            'sge_template_name': self.sge_template_name,
            'job_setter': self._job_setter,
            'job_finished_checker': self._job_finished_checker,
        }

    def __setstate__(self, state):
        self.ssh_host = state['ssh_host']
        self.cores = state['cores']
        self.programs = state['programs']
        self.sge_template_name = state['sge_template_name']
        self.set_job_id = aux.load_function_from_module(state['job_setter'])
        self.check_task_finished = aux.load_function_from_module(state['job_finished_checker'])

    def join_path(self, *path_list):
        # For RemoteHost, we assume that it is posix-based
        return '/'.join(path_list)


# Decorator
def enable_sftp(func):
    def wrapped_func(self, *args, **kwds):
        self._init_sftp()
        return func(self, *args, **kwds)
    return wrapped_func


class BaseCommunication(metaclass=ABCMeta):
    '''
    BaseCommunication is an abstract class which can be used to implement the simplest access to a machine.
    A concrete class ought to use a concrete method of communication (e.g., OS API or ssh) allowing to access 
    the filesystem (copy and remove files) and execute a command line on the machine.

    Since a machine can be, in particular, the local machine, and at the same time we must always establish the communication between
    the local machine and a machine being communicated, we have to sort the terminology out. We shall call the latter a communicated 
    machine whilst the former remain the local machine.

    Generally, two types of files exchange are possible:
    (1) between the local machine and a communicated machine,
    (2) within a communicated machine.
    Since for now only copying implies this division, we introduce so called 'modes of copying': from_local, to_local 
    and all_on_communicated
    '''

    def __init__(self, host, machine_name):
        self.host = host
        self.machine_name = machine_name

    @abstractmethod
    def execute(self, command, working_dir=None):
        pass

    @abstractmethod
    def copy(self, from_, to_, mode='from_local', show_msg=False):
        '''
        Copies from_ to to_ which are interpreted according to mode:
        (1) from_local (default) -> from_ is local path, to_ is a path on a communicated machine
        (2) from_remote -> from_ is a path on a communicated machine, to_ local path
        (3) all_remote -> from_ and to_ are paths on a communicated machine

        from_ and to_ can be dirs or files according to the following combinations:
        (1) from_ is dir, to_ is dir
        (2) from_ is file, to_ is dir
        (3) from_ is file, to_ is file
        '''
        pass

    @abstractmethod
    def rm(self, target):
        '''
        Removes target which can be a dir or file
        '''
        pass

    def execute_program(self, prog_name, args_str, working_dir=None, chaining_command_at_start='',
                        chaining_command_at_end=''):
        prog_path = self.host.get_program_launch_path(prog_name)
        command = f'{chaining_command_at_start} {prog_path} {args_str} {chaining_command_at_end}'
        return self.execute(command, working_dir)

    def _print_copy_msg(self, from_, to_):
        print('\tCopying %s to %s' % (from_, to_))

    def _print_exec_msg(self, cmd, is_remote):
        where = '@' + self.machine_name if is_remote else ''
        print('\tExecuting %s: %s' % (where, cmd))


class LocalCommunication(BaseCommunication):
    def __init__(self, local_host, machine_name='laptop'):
        super(LocalCommunication, self).__init__(local_host, machine_name)

    @classmethod
    def create_from_config(cls):
        with open(os.path.expanduser('~/.comsdk/config_research.json'), 'r') as f:
            conf = json.load(f)
        local_host = Host()
        _add_programs_and_commands_from_config(local_host, conf['LOCAL_HOST'])
        return LocalCommunication(local_host)

    def execute(self, command, working_dir=None):
        if working_dir is None:
            command_line = command 
        else:
            if os.name == 'posix':
                command_line = 'cd {}; {}'.format(working_dir, command)
            elif os.name == 'nt':
                command_line = ''
                if working_dir[0] != 'C':
                    command_line += f'{working_dir[0]}: && '
                command_line += 'cd {} && {}'.format(working_dir, command)
        #self._print_exec_msg(command_line, is_remote=False)
        #res = subprocess.call([command_line], shell=True)
    #    print(command_line)
        res = subprocess.run(command_line, shell=True)
        return [], []

    def copy(self, from_, to_, mode='from_local', show_msg=False):
        '''
        Any mode is ignored since the copying shall be within a local machine anyway
        '''
        if show_msg:
            self._print_copy_msg(from_, to_)
        return cp(from_, to_)

    def rm(self, target):
        aux.rm(target)


class SshCommunication(BaseCommunication):
    def __init__(self, remote_host, username, password, machine_name='', pkey=None, execute_after_connection=None):
        if not isinstance(remote_host, RemoteHost):
            Exception('Only RemoteHost can be used to build SshCommunication')
        self.host = remote_host
        self.username = username
        self.password = password
        self.pkey = pkey
        self.execute_after_connection = execute_after_connection
        self.ssh_client = paramiko.SSHClient()
        self.sftp_client = None
        #self.main_dir = '/nobackup/mmap/research'
        super().__init__(self.host, machine_name)
        self.connect()
        paramiko.util.log_to_file('paramiko.log')

    @classmethod
    def create_from_config(cls, host_sid):
        with open(os.path.expanduser('~/.comsdk/config_research.json'), 'r') as f:
            conf = json.load(f)
        hostconf = conf['REMOTE_HOSTS'][host_sid]
        remote_host = RemoteHost(ssh_host=hostconf['ssh_host'],
                                 cores=hostconf['max_cores'],
                                 sge_template_name=hostconf['sge_template_name'],
                                 job_setter=hostconf['job_setter'],
                                 job_finished_checker=hostconf['job_finished_checker'])
        _add_programs_and_commands_from_config(remote_host, hostconf)
        return SshCommunication(remote_host, username=hostconf['username'],
                                             password=hostconf['password'] if 'password' in hostconf else None,
                                             machine_name=host_sid,
                                             pkey=hostconf['pkey'] if 'pkey' in hostconf else None,
                                             execute_after_connection=hostconf['execute_after_connection'] if 'execute_after_connection' in hostconf else None)

    def __getstate__(self):
        return {
            'host': self.host.__getstate__(),
            'username': self.username,
            'password': self.password,
            'pkey': self.pkey,
            'execute_after_connection': self.execute_after_connection,
        }

    def __setstate__(self, state):
        remote_host = RemoteHost.__new__(RemoteHost)
        remote_host.__setstate__(state['host'])
        self.__init__(remote_host, state['username'], state['password'], pkey=state['pkey'],
                      execute_after_connection=state['execute_after_connection'])

    def execute(self, command, working_dir=None):
        if self.ssh_client is None:
            raise Exception('Remote host is not set')

        self._print_exec_msg(command, is_remote=True)
        command_line = command if working_dir is None else 'cd {}; {}'.format(working_dir, command)
        command_line = command_line if self.execute_after_connection is None else f'{self.execute_after_connection}; {command_line}'
        print(command_line)

        def _cleanup():
            print('\t\tMSG: Reboot SSH client')
            self.reboot()
        cleanup = _cleanup
        received = False
        while not received:
            try:
                stdin, stdout, stderr = self.ssh_client.exec_command(command_line)
                received = True
            except (OSError, socket.timeout, socket.error, paramiko.sftp.SFTPError) as e:
                print('\t\tMSG: Catched {} exception while executing "{}"'.format(type(e).__name__, command_line))
                print('\t\tMSG: It says: {}'.format(e))
            else:
                cleanup = lambda: None
            cleanup()
        for line in stdout:
            print('\t\t' + line.strip('\n'))
        for line in stderr:
            print('\t\t' + line.strip('\n'))
        return stdout.readlines(), stderr.readlines()

    def copy(self, from_, to_, mode='from_local', show_msg=False):
        if self.ssh_client is None:
            raise Exception('Remote host is not set')
        self._init_sftp()

        new_path = None
        if mode == 'from_local':
            new_path = self._copy_from_local(from_, to_, show_msg)
        elif mode == 'from_remote':
            new_path = self._copy_from_remote(from_, to_, show_msg)
        elif mode == 'all_remote':
            if show_msg:
                self._print_copy_msg(self.machine_name + ':' + from_, self.machine_name + ':' + to_)
            self._mkdirp(to_)
            self.execute('cp -r %s %s' % (from_, to_))
        else:
            raise Exception("Incorrect mode '%s'" % mode)
        return new_path

    def rm(self, target):
        if self.ssh_client is None:
            raise Exception('Remote host is not set')
        self._init_sftp()
        self.execute('rm -r %s' % target)

    @enable_sftp
    def mkdir(self, path):
        self.sftp_client.mkdir(path)

    @enable_sftp
    def listdir(self, path_on_remote):
        return self.sftp_client.listdir(path_on_remote)

    @enable_sftp
    def _chdir(self, path=None):
        self.sftp_client.chdir(path)

    @enable_sftp
    def _mkdirp(self, path):
        path_list = path.split('/')
        cur_dir = ''
        if (path_list[0] == '') or (path_list[0] == '~'): # path is absolute and relative to user's home dir => don't need to check obvious
            cur_dir = path_list.pop(0) + '/'
        start_creating = False # just to exclude unnecessary stat() calls when we catch non-existing dir
        for dir_ in path_list:
            if dir_ == '': # trailing slash or double slash, can skip
                continue
            cur_dir += dir_
            if start_creating or (not self._is_remote_dir(cur_dir)):
                self.mkdir(cur_dir)
                if not start_creating:
                    start_creating = True

            cur_dir += '/'

    @enable_sftp
    def _open(self, filename, mode='r'):
        return self.sftp_client.open(filename, mode)

    @enable_sftp
    def _get(self, remote_path, local_path):
        def _cleanup():
            print('\t\tMSG: Reboot SSH client')
            self.reboot()
            if os.path.exists(local_path):
                aux.rm(local_path)

        cleanup = _cleanup
        received = False
        while not received:
            try:
                res = self.sftp_client.get(remote_path, local_path)
                received = True
            except FileNotFoundError as e:
                logging.error('Cannot find file or directory "{}" => interrupt downloading'.format(remote_path))
                if os.path.exists(local_path):
                    aux.rm(local_path)
                raise
            except (socket.timeout, socket.error, paramiko.sftp.SFTPError) as e:
                print('\t\tMSG: Catched {} exception while getting "{}"'.format(type(e).__name__, remote_path))
                print('\t\tMSG: It says: {}'.format(e))
            else:
                cleanup = lambda: None
            cleanup()
        return res

    @enable_sftp
    def _put(self, local_path, remote_path):
        def _cleanup():
            print('\t\tMSG: Reboot SSH client')
            self.reboot()
            self.rm(remote_path)
        cleanup = _cleanup
        received = False
        while not received:
            try:
                res = self.sftp_client.put(local_path, remote_path)
                received = True
            except FileNotFoundError as e:
                logging.error('Cannot find file or directory "{}" => interrupt uploading'.format(local_path))
                self.rm(remote_path)
                raise
            except (socket.timeout, socket.error, paramiko.sftp.SFTPError) as e:
                print('\t\tMSG: Catched {} exception while putting "{}"'.format(type(e).__name__, remote_path))
                print('\t\tMSG: It says: {}'.format(e))
            else:
                cleanup = lambda: None
            cleanup()
        return res

    def _is_remote_dir(self, path):
        try:
            return S_ISDIR(self.sftp_client.stat(path).st_mode)
        except IOError:
            return False

    def _copy_from_local(self, from_, to_, show_msg=False):
        new_path_on_remote = to_ + '/' + os.path.basename(from_)
        if os.path.isfile(from_):
            self._mkdirp(to_)
            if show_msg:
                self._print_copy_msg(from_, self.machine_name + ':' + to_)
            self._put(from_, new_path_on_remote)
        elif os.path.isdir(from_):
            self.mkdir(new_path_on_remote)
            for dir_or_file in os.listdir(from_):
                self._copy_from_local(os.path.join(from_, dir_or_file), new_path_on_remote, show_msg)
        else:
            raise CommunicationError("Path %s does not exist" % from_)
        return new_path_on_remote

    def _copy_from_remote(self, from_, to_, show_msg=False):
        new_path_on_local = os.path.join(to_, os.path.basename(from_))
        if not self._is_remote_dir(from_):
            if show_msg:
                self._print_copy_msg(self.machine_name + ':' + from_, to_)
            self._get(from_, new_path_on_local)
        else:
            os.mkdir(new_path_on_local)
            for dir_or_file in self.sftp_client.listdir(from_):
                self._copy_from_remote(from_ + '/' + dir_or_file, new_path_on_local, show_msg)
        return new_path_on_local

    def disconnect(self):
        if self.sftp_client is not None:
            self.sftp_client.close()
            self.sftp_client = None
        self.ssh_client.close()

    def connect(self):
        self.ssh_client.load_system_host_keys()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        connected = False

        # read ssh config. We assume that all necessary re-routing are done there via ProxyCommand
        # only ProxyCommand is read; password should be passed explicitly to SshCommunication
        ssh_config = paramiko.SSHConfig()
        user_config_file = os.path.expanduser("~/.ssh/config")
        if os.path.exists(user_config_file):
            with open(user_config_file) as f:
                ssh_config.parse(f)

        user_config = ssh_config.lookup(self.host.ssh_host)
        sock = None
        if 'proxycommand' in user_config:
            sock = paramiko.ProxyCommand(user_config['proxycommand'])

        while not connected:
            try:
                if self.pkey is not None: # if a private key is given, first attempt to connect using it
                    self.ssh_client.connect(self.host.ssh_host, username=self.username, key_filename=self.pkey, timeout=10, sock=sock)
                else: # otherwise try to connect via password using it is given
                    print(self.host.ssh_host, self.username)
                    self.ssh_client.connect(self.host.ssh_host, username=self.username, password=self.password, look_for_keys=False, allow_agent=False, timeout=10, sock=sock)
                connected = True
            except socket.timeout as e:
                print('\t\tMSG: Catched {} exception while connecting'.format(type(e).__name__))
                print('\t\tMSG: It says: {}'.format(e))

        transport = self.ssh_client.get_transport()
        transport.packetizer.REKEY_BYTES = pow(2, 40) # 1TB max, this is a security degradation (otherwise we get "paramiko.ssh_exception.SSHException: Key-exchange timed out waiting for key negotiation")
        transport.packetizer.REKEY_PACKETS = pow(2, 40) # 1TB max, this is a security degradation (otherwise we get "paramiko.ssh_exception.SSHException: Key-exchange timed out waiting for key negotiation")

        if self.execute_after_connection is not None:
            self.execute(self.execute_after_connection)

    def reboot(self):
        self.disconnect()
        self.connect()
        self._init_sftp()

    def _init_sftp(self):
        if self.sftp_client is None:
            self.sftp_client = self.ssh_client.open_sftp()
            self.sftp_client.get_channel().settimeout(10)


class CommunicationError(Exception):
    pass


def _add_programs_and_commands_from_config(host, hostconf):
    if 'custom_programs' in hostconf:
        paths = hostconf['custom_programs']
        for path, programs in paths.items():
            for program in programs:
                host.add_program(program, path)
    if 'env_programs' in hostconf:
        for program in hostconf['env_programs']:
            host.add_program(program)
    if 'custom_commands' in hostconf:
        for cmd_name, cmd in hostconf['custom_commands'].items():
            host.add_command(cmd_name, cmd)

