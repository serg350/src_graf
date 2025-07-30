from comsdk.misc import find_dir_by_named_regexp
from functools import partial
import os


class DistributedStorage:
    """
    Distributed storage is a set of sources contaning the data. The sources must be accessible by the OS API.
    It is assumed that the data somewhat overlaps, namely, it should overlap in terms of the catalog hierarchy. 
    However, this implementation does not guarantee the uniqueness of data: instead, it uses a priority to prefer 
    one source over another while looking up. Even though duplicates are acceptable, the found ones will be printed
    out for the sake of user's attention. 
    """
    def __init__(self, abs_storage_paths, prior_storage_index=0):
        self.storage_paths = abs_storage_paths
        self.prior_storage_index = prior_storage_index

    def get_dir_path(self, dir_):
        """
        Returns the full path to dir_ or None if dir_ is absent.
        """
        dir_path_tuple = self.lookup_through_dir(dir_, lambda dir_path: (dir_path, dir_path)
                                                        if os.path.exists(dir_path) else None)
        return dir_path_tuple[0] if dir_path_tuple is not None else None

    def make_dir(self, dir_):
        """
        Creates dir_ in prior storage. Returns the full path to it.
        """
        path_ = os.path.join(self.storage_paths[self.prior_storage_index], dir_)
        os.makedirs(path_)
        return path_

    def find_dir_by_named_regexp(self, parent_dir, regexp):
        """
        Finds a directory in parent_dir fulfulling regexp. Returns a tuple (full_path_to_found_dir, named_params_from_regexp).
        """
        return self.lookup_through_dir(parent_dir, partial(find_dir_by_named_regexp, regexp))

    def lookup_through_dir(self, dir_, lookup_func):
        """
        Looks up the data in dir_ by executing lookup_func on dir_. Returns a tuple (full_path_to_dir, some_data_regarding_dir) 
        which must, in turn, be returned by lookup_func. lookup_func must take a single argument -- full path to the dir. 
        """
        possible_paths = [os.path.join(source, dir_) if dir_ != '' else source for source in self.storage_paths]
        found_data = None
        prior_found = False
        for path_i in range(len(possible_paths)):
            path_ = possible_paths[path_i]
            if os.path.exists(possible_paths[path_i]):
                tmp_found_data = lookup_func(possible_paths[path_i])
                if tmp_found_data is not None:
                    tmp_found_path = os.path.join(possible_paths[path_i], tmp_found_data[0])
                    if found_data is not None:
                        print("Duplicate distributed dir is found: '{}' and '{}'".format(tmp_found_path, found_data[0]))
                    if not prior_found:
                        found_data = (tmp_found_path, tmp_found_data[1])
                    if path_i == self.prior_storage_index:
                        prior_found = True
        return found_data

    def listdir(self, dir_):
        """
        Lists the content of dir_. Returns a tuple (dirnames, filenames) which are obtained by simple union of the content of sources.
        Therefore, there might be copies whose detection must be performed elsewhere.
        """
        dirnames = []
        filenames = []
        for storage_path in self.storage_paths:
            if os.path.exists(os.path.join(storage_path, dir_)):
                _, dirnames_, filenames_ = next(os.walk(os.path.join(storage_path, dir_)))
                dirnames += dirnames_
                filenames += filenames_
        return dirnames, filenames
