#!/usr/bin/env python
"""
rsync snapshot backups
"""

import argparse
import datetime
import operator
import os
import re
import shlex
import subprocess


class RsyncBackuper(object):
    def __init__(self, rsync_args):
        self.rsync_args = rsync_args
        self.dests = self._list_previous_dests()

    def backup(self, sources):
        raise NotImplementedError()

    def complete_dest(self, dest):
        raise NotImplementedError()

    def _list_previous_dests(self):
        raise NotImplementedError()

    @classmethod
    def create(cls, args):
        if ":" in args.root:
            # username@remote_host:path
            tmp = args.root.split(":")
            if len(tmp) != 2:
                raise Exception("illegal use of : in the root path: %s" % args.root)

            remote_string = tmp[0]
            root = tmp[1]

            tmp = remote_string.split("@")
            if len(tmp) == 1:
                user = None
                host = tmp[0]
            elif len(tmp) == 2:
                user = tmp[0]
                host = tmp[1]
            else:
                raise Exception("illegal use of @ in the root path: %s" % args.root)

            return RsyncBackuper_Remote(root, host, user, args.rsh, args.rsync_args)
        else:
            root = os.path.abspath(args.root)
            if not os.path.exists(root) or not os.path.isdir(root):
                raise ValueError("no such dir: {0}".format(args.root))

            return RsyncBackuper_Local(root, args.rsync_args)


class RsyncBackuper_Remote(RsyncBackuper):
    def __init__(self, root, host, user, rsh, rsync_args):
        self.root = root
        self.host = host
        self.user = user

        rsh = shlex.split(rsh)
        if self.user is not None:
            rsh += ["%s@%s" % (self.user, self.host)]
        else:
            rsh += self.host
        self.rsh = rsh

        super(RsyncBackuper_Remote, self).__init__(rsync_args)

    def _list_previous_dests(self):
        cmd_info = self.rsh + ["./rsnap2.py info {}".format(self.root)]
        info_str = subprocess.check_output(cmd_info)
        info_lines = info_str.splitlines()

        previous_dests = []
        for info_line in info_lines:
            previous_dests.append(RsyncBackuperDest.parse(info_line, self.root))

        previous_dests.sort(key=operator.attrgetter("time"), reverse=True)

        return previous_dests


class RsyncBackuper_Local(RsyncBackuper):
    def __init__(self, root, rsync_args):
        assert os.path.isabs(root)
        self.root = root
        super(RsyncBackuper_Local, self).__init__(rsync_args)

    def backup(self, sources):

        dest = RsyncBackuperDest.new(self.root)
        previous_backup = None
        if len(self.dests) > 0:
            previous_backup = next((x for x in self.dests if x.is_complete), None)

        rsync_call = ["rsync"] + sources + [dest.path]
        rsync_call += self.rsync_args
        if previous_backup is not None:
            rsync_call += ["--link-dest", previous_backup.path]

        print("issuing: ", rsync_call)
        print("---")
        try:
            subprocess.check_call(rsync_call)
        except subprocess.CalledProcessError, e:
            raise e

        if not(os.path.exists(dest.path) and os.path.isdir(dest.path)):
            raise Exception("rsync doesn't seem to have backed up your data, please check command line arguments!")

        self.complete_dest(dest)

    def complete_dest(self, dest):
        assert dest.root == self.root

        path_old = dest.path

        dest.is_complete = True
        dest.dirname = dest._get_dirname(dest.time)
        path_new = dest.path

        os.rename(path_old, path_new)

    def _list_previous_dests(self):
        previous_dests = []

        root_subdirs = [child for child in os.listdir(self.root) if os.path.isdir(os.path.join(self.root, child))]
        for root_subdir in root_subdirs:
            dest = RsyncBackuperDest.parse(root_subdir, self.root)

            if dest is not None:
                previous_dests.append(dest)

        previous_dests.sort(key=operator.attrgetter("time"), reverse=True)

        return previous_dests


class RsyncBackuperDest:
    DEST_DIR_FORMAT = "{time}{dotflag}"
    DEST_DIR_DATE_FORMAT = "%Y-%m-%d_%H-%M-%S.%f"
    DEST_DIR_RE = re.compile(r"^(?P<time>\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}.\d{6})(?P<flag>\.partial)?$")

    def __init__(self, dirname, root, time, is_complete):
        self.dirname = dirname
        self.root = root
        self.time = time
        self.is_complete = is_complete

    @property
    def path(self):
        return os.path.join(self.root, self.dirname)

    @classmethod
    def parse(cls, dirname, root):
        match = cls.DEST_DIR_RE.match(dirname)
        if match is None:
            return None

        time = datetime.datetime.strptime(match.group("time"), cls.DEST_DIR_DATE_FORMAT)

        flag = match.group("flag")
        is_complete = flag is None

        return cls(dirname=dirname, root=root, time=time, is_complete=is_complete)

    @classmethod
    def new(cls, root):
        time = datetime.datetime.now()
        dirname = cls._get_dirname(time, "partial")

        return cls(dirname=dirname, root=root, time=time, is_complete=False)

    @classmethod
    def _get_dirname(cls, time, flag=None):
        return cls.DEST_DIR_FORMAT.format(
            time=time.strftime(cls.DEST_DIR_DATE_FORMAT),
            dotflag="" if flag is None else ".{}".format(flag)
        )

def BackupAction(args):
    if True in (arg.startswith("--link-dest") for arg in args.rsync_args):
        raise ValueError("--link-dest cannot be overwritten in --rsync-args")

    backuper = RsyncBackuper.create(args)
    backuper.backup(args.sources)
    return 0

def InfoAction(args):
    backuper = RsyncBackuper.create(args)

    dests = [dest.dirname for dest in backuper.dests]
    print("{}".format("\n".join(dests)))
    return 0


if __name__ == "__main__":
    args_parser = argparse.ArgumentParser(description="rsync snapshot backups")

    actions_parsers = args_parser.add_subparsers()
    actions_parent = argparse.ArgumentParser(add_help=False)
    actions_parent.add_argument("--rsh")
    actions_parent.add_argument("--rsync-args", nargs=argparse.REMAINDER, default=["-a", "-v"])

    action_backup = actions_parsers.add_parser("backup", parents=[actions_parent])
    action_backup.add_argument("sources", nargs="+")
    action_backup.add_argument("root")
    action_backup.set_defaults(handler=BackupAction)

    action_info = actions_parsers.add_parser("info", parents=[actions_parent])
    action_info.add_argument("root")
    action_info.set_defaults(handler=InfoAction)

    args = args_parser.parse_args()
    exit(args.handler(args))
