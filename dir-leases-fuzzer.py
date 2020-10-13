import os

import logging

import sys

from multiprocessing import Process

from contextlib import contextmanager

from random import randint

from easypy.random import choose, random_filename, random_buf

from logging import getLogger, INFO
logger = getLogger()
logger.setLevel(INFO)
logging.basicConfig(stream=sys.stdout)


class DirLeaseFuzzer(object):
    def __init__(self, wekafs, localfs='/mnt/localfs'):
        random_suffix = random_filename(3)
        dir_name = '/dir_leases_directory_' + random_suffix

        self.localfs_path = localfs + dir_name
        os.makedirs(self.localfs_path, exist_ok=True)

        self.wekafs_path = wekafs + dir_name
        os.makedirs(self.wekafs_path, exist_ok=True)

        alternate_wekafs_mount_point = '/mnt/wekafs'
        os.makedirs(alternate_wekafs_mount_point, exist_ok=True)
        os.system('mount -t wekafs default %s -o dentry_max_age_positive=0' % alternate_wekafs_mount_point)
        self.wekafs_alternate_path = alternate_wekafs_mount_point + dir_name
        os.makedirs(self.wekafs_alternate_path, exist_ok=True)

        logger.info("Mount-paths are:\n%s\n%s\n%s\n" % (self.localfs_path, self.wekafs_path, self.wekafs_alternate_path))

        self.dir_contents = dict()

    def validate_same_behavior_on_both_paths(self, func, wekafs_alternate_path, **kwargs):
        if kwargs:
            logger.info("Extra arguments: %s" % kwargs)

        wekafs_path = self.wekafs_alternate_path if wekafs_alternate_path else self.wekafs_path

        try:
            res_1 = func(self.localfs_path, **kwargs)
        except Exception as e_1:
            # path_1 got exception, expecting path_2 to get the same exception:
            try:
                func(wekafs_path, **kwargs)
            except Exception as e_2:
                msg = "Expected both paths to get the same exception but %s got %s and %s got %s" % (self.localfs_path, e_1, wekafs_path, e_2)
                assert e_1.args == e_2.args, msg
            else:
                # path_1 got exception but Path_2 did not
                assert False, "Expected both paths to get the same exception but %s got %s and %s did not get any exception" % (self.localfs_path, e_1, wekafs_path)
        else:
            # path_1 didn't get exception, expecting same result in path_2
            res_2 = func(wekafs_path, **kwargs)
            assert res_1 == res_2, "Expected both paths to return same result but %s got %s and %s got %s" % (self.localfs_path, res_1, wekafs_path, res_2)
            return res_2

    def choose_action(self):
        possible_actions = []

        def list_dir(self, wekafs_alternate_path):
            wekafs_path = self.wekafs_alternate_path if wekafs_alternate_path else self.wekafs_path
            wekafs_res = os.listdir(self.localfs_path)
            logger.debug('listdir according to wekafs: %s' % wekafs_res)
            localfs_res = os.listdir(wekafs_path)
            logger.debug('listdir according to localfs: %s' % localfs_res)
            assert set(wekafs_res) == set(localfs_res), 'Inconsistency between wekafs and localfs. see debug logs for info'
            return wekafs_res
        possible_actions.append(list_dir)

        def create_file(self, wekafs_alternate_path):
            def _mknod(path, filename):
                os.mknod(path + '/' + filename)
                return filename
            filename = choose([k for (k, v) in self.dir_contents.items()] + [random_filename()])
            new_file = self.validate_same_behavior_on_both_paths(_mknod, wekafs_alternate_path, filename=filename)
            if new_file:
                # this means there was no exception
                wekafs_path = self.wekafs_alternate_path if wekafs_alternate_path else self.wekafs_path
                wekafs_inode = os.stat(wekafs_path + '/' + new_file).st_ino
                self.dir_contents[new_file] = wekafs_inode
        possible_actions.append(create_file)

        def remove_file(self, wekafs_alternate_path):
            def _rm(path, filename):
                os.remove(path + '/' + filename)
                return filename
            filename = choose([k for (k, v) in self.dir_contents.items()] + [random_filename()])
            del_file = self.validate_same_behavior_on_both_paths(_rm, wekafs_alternate_path, filename=filename)
            if del_file:
                self.dir_contents.pop(del_file)
        possible_actions.append(remove_file)

        def create_dir(self, wekafs_alternate_path):
            def _mkdir(path, dirname):
                os.mkdir(path + '/' + dirname)
                return dirname
            dirname = choose([k for (k, v) in self.dir_contents.items()] + [random_filename()])
            new_dir = self.validate_same_behavior_on_both_paths(_mkdir, wekafs_alternate_path, dirname=dirname)
            if new_dir:
                # this means there was no exception
                wekafs_path = self.wekafs_alternate_path if wekafs_alternate_path else self.wekafs_path
                wekafs_inode = os.stat(wekafs_path + '/' + new_dir).st_ino
                self.dir_contents[new_dir] = wekafs_inode
        possible_actions.append(create_dir)

        def remove_dir(self, wekafs_alternate_path):
            def _rmdir(path, dirname):
                os.rmdir(path + '/' + dirname)
                return dirname
            filename = choose([k for (k, v) in self.dir_contents.items()] + [random_filename()])
            del_dir = self.validate_same_behavior_on_both_paths(_rmdir, wekafs_alternate_path, dirname=filename)
            if del_dir:
                self.dir_contents.pop(del_dir)
        possible_actions.append(remove_dir)

        def create_link(self, wekafs_alternate_path):
            def _link(path, filename, linkname):
                os.link(path + '/' + filename, path + '/' + linkname)
                return linkname
            filename = choose([k for (k, v) in self.dir_contents.items()] + [random_filename()])
            new_link = self.validate_same_behavior_on_both_paths(_link, wekafs_alternate_path, filename=filename, linkname=random_filename())
            if new_link:
                # this means there was no exception
                wekafs_path = self.wekafs_alternate_path if wekafs_alternate_path else self.wekafs_path
                wekafs_inode = os.stat(wekafs_path + '/' + new_link).st_ino
                self.dir_contents[new_link] = wekafs_inode
        possible_actions.append(create_link)

        def stat_file(self, wekafs_alternate_path):
            def _stat(path, filename):
                stats = os.stat(path + '/' + filename)
                mode = stats.st_mode
                # atime, mtime and ctime might differ by a few nanoseconds
                # inode and device will be different between the 2 filesystems
                # directory's nlink is different between wekafs and posix (see WEKAPP-76966)
                # directory's size is unspecified
                # this only leaves the 'mode' stat
                return mode
            filename = choose([k for (k, v) in self.dir_contents.items()] + [random_filename()])
            ret = self.validate_same_behavior_on_both_paths(_stat, wekafs_alternate_path, filename=filename)
            if ret:
                # this means there was no exception
                wekafs_path = self.wekafs_alternate_path if wekafs_alternate_path else self.wekafs_path
                wekafs_inode = os.stat(wekafs_path + '/' + filename).st_ino
                assert wekafs_inode == self.dir_contents[filename]
        possible_actions.append(stat_file)

        def access_file(self, wekafs_alternate_path):
            def _access(path, filename, flag):
                return os.access(path + '/' + filename, flag)
            filename = choose([k for (k, v) in self.dir_contents.items()] + [random_filename()])
            flag = choose([os.R_OK, os.W_OK, os.X_OK])
            self.validate_same_behavior_on_both_paths(_access, wekafs_alternate_path, filename=filename, flag=flag)
        possible_actions.append(access_file)

        def chmod_file(self, wekafs_alternate_path):
            def _chmod(path, filename, value):
                os.chmod(path + '/' + filename, value)
                return (filename, value)
            filename = choose([k for (k, v) in self.dir_contents.items()] + [random_filename()])
            value = randint(0, 0o777)
            self.validate_same_behavior_on_both_paths(_chmod, wekafs_alternate_path, filename=filename, value=value)
        possible_actions.append(chmod_file)

        def open_read_write_close(self, wekafs_alternate_path):
            def _open(path, filename, buf):
                @contextmanager
                def opened_file(filename):
                    fd = os.open(filename, flags=os.O_RDWR)
                    yield fd
                    os.close(fd)
                with opened_file(filename) as fd:
                    os.read(fd, 100)
                    os.write(fd, buf)
                return (filename, buf)
            filename = choose([k for (k, v) in self.dir_contents.items()] + [random_filename()])
            buf = random_buf(100)
            self.validate_same_behavior_on_both_paths(_open, wekafs_alternate_path, filename=filename, buf=buf)
        possible_actions.append(open_read_write_close)

        chosen_action = choose(possible_actions)
        return chosen_action

    def run_test(self, num_actions):
        for i in range(num_actions):
            chosen_action = self.choose_action()
            logger.info("Starting action #%d - %s" % (i + 1, chosen_action.__name__))
            wekafs_alternate_path = i % 2 == 0  # alternating the path in each other action
            chosen_action(self, wekafs_alternate_path=wekafs_alternate_path)

num_actions = 1000
num_procs = 1

if num_procs == 1:
    fuzzer = DirLeaseFuzzer('/wekakwfs/default--mode-driver-readcache--dentry_max_age_positive-0', localfs='/mnt/localfs')
    fuzzer.run_test(num_actions)
else:
    procs = []
    for i in range(num_procs):
        fuzzer = DirLeaseFuzzer('/wekakwfs/default--mode-driver-readcache--dentry_max_age_positive-0', localfs='/mnt/localfs')
        proc = Process(target=fuzzer.run_test, args=(num_actions,))
        procs.append(proc)

    for p in procs:
        p.start()

    for p in procs:
        p.join()
