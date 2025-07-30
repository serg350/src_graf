import os
import subprocess

class BaseEnvironment(object):
    def __init__(self):
        self._programs = {}

    def preprocess(self, working_dir, input_copies_list):
        raise NotImplementedError()

    def execute(self, working_dir, prog_name, command_line):
        raise NotImplementedError()

    def postprocess(self, working_dir, output_copies_list):
        raise NotImplementedError()

    def add_program(self, prog_name, path_to_prog):
        self._programs[prog_name] = path_to_prog

#    def _print_copy_msg(self, from_, to_):
#        print('\tCopying %s to %s' % (from_, to_))
#
#    def _print_exec_msg(self, cmd, is_remote):
#        where = '@' + self._machine_name if is_remote else ''
#        print('\tExecuting %s: %s' % (where, cmd))

class LocalEnvironment(BaseEnvironment):
    def __init__(self):
        super().__init__()

    def preprocess(self, working_dir, input_copies_list):
        for copy_target in input_copies_list:
            _copy(self, copy_target, working_dir)

    def execute(self, working_dir, prog_name, args_str):
        prog_path = os.path.join(self._programs[prog_name], prog_name)
        command_line = 'cd {}; {} {}'.format(working_dir, prog_path, args_str)
        # use PIPEs to avoid breaking the child process when the parent process finishes
        # (works on Linux, solution for Windows is to add creationflags=0x00000010 instead of stdout, stderr, stdin)
 #       self._print_exec_msg(command_line, is_remote=False)
        #pid = subprocess.Popen(args, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
        #print(pid)
        subprocess.call([command_line], shell=True)

    def postprocess(self, working_dir, output_copies_list):
        pass

    def _copy(self, from_, to_, mode='from_local'):
        """Any mode is ignored since the copying shall be within a local machine anyway
        """
        cp(from_, to_)
        self._print_copy_msg(from_, to_)

    def rm(self, target):
        rm(target)
