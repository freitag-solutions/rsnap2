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
import sys


class RsyncBackuper(object):
    def __init__(self, root, rsync_args):
        self.root = root
        self.rsync_args = rsync_args

        dests = self._list_previous_dests()
        dests.sort(key=operator.attrgetter("time"), reverse=True)

        self.dests = dests

    def backup(self, sources):
        dest = RsyncBackuperDest.new(self.root)
        previous_backup = None
        if len(self.dests) > 0:
            previous_backup = next((x for x in self.dests if x.is_complete), None)

        rsync_params = sources + [dest.path]
        rsync_params += self.rsync_args
        if previous_backup is not None:
            rsync_params += ["--link-dest", previous_backup.path]
        self._issue_rsync(rsync_params)

        self._complete_dest(dest)

    def _issue_rsync(self, params):
        raise NotImplementedError()

    def _list_previous_dests(self):
        raise NotImplementedError()

    def _complete_dest(self, dest):
        raise NotImplementedError()

    @classmethod
    def create(cls, root, rsync_args, rsh, rsh_rsnap2):
        if ":" in root:
            # username@remote_host:path
            tmp = root.split(":")
            if len(tmp) != 2:
                raise Exception("illegal use of : in the root path: %s" % root)

            remote_host_string = tmp[0]
            remote_root = tmp[1]

            tmp = remote_host_string.split("@")
            if len(tmp) == 1:
                user = None
                host = tmp[0]
            elif len(tmp) == 2:
                user = tmp[0]
                host = tmp[1]
            else:
                raise Exception("illegal use of @ in the root path: %s" % root)

            if rsh is None:
                raise Exception("--rsh must be given when targeting a remote repository")

            return RsyncBackuper_Remote(root, remote_root, host, user, rsh, rsh_rsnap2, rsync_args)
        else:
            root = os.path.abspath(root)
            if not os.path.exists(root) or not os.path.isdir(root):
                raise ValueError("no such dir: {0}".format(root))

            return RsyncBackuper_Local(root, rsync_args)


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


class RsyncBackuper_Local(RsyncBackuper):
    def __init__(self, root, rsync_args):
        super(RsyncBackuper_Local, self).__init__(root, rsync_args)

    def _issue_rsync(self, params):
        rsync_call = ["rsync"] + params

        print "issuing: ", rsync_call
        print "---"
        try:
            subprocess.check_call(rsync_call)
        except subprocess.CalledProcessError, e:
            raise e

    def _list_previous_dests(self):
        previous_dests = []

        root_subdirs = [child for child in os.listdir(self.root) if os.path.isdir(os.path.join(self.root, child))]
        for root_subdir in root_subdirs:
            dest = RsyncBackuperDest.parse(root_subdir, self.root)

            if dest is not None:
                previous_dests.append(dest)

        return previous_dests

    def _complete_dest(self, dest):
        assert dest.root == self.root

        if not(os.path.exists(dest.path) and os.path.isdir(dest.path)):
            raise Exception("destination doesn't exist! maybe check your rsync arguments?")

        path_old = dest.path

        dest.is_complete = True
        dest.dirname = dest._get_dirname(dest.time)
        path_new = dest.path

        os.rename(path_old, path_new)


class RsyncBackuper_Remote(RsyncBackuper):
    def __init__(self, root, remote_root, host, user, rsh, rsh_rsnap2, rsync_args):
        self.remote_root = remote_root
        self.host = host
        self.user = user
        self.rsnap2 = "rsnap2" if rsh_rsnap2 is None else rsh_rsnap2

        self.rsh_orig = rsh
        rsh = shlex.split(rsh)
        if self.user is not None:
            rsh += ["%s@%s" % (self.user, self.host)]
        else:
            rsh += self.host
        self.rsh = rsh

        super(RsyncBackuper_Remote, self).__init__(root, rsync_args)

    def _issue_rsync(self, params):
        rsync_call = ["rsync"] + ["--rsh", self.rsh_orig] + params

        print "issuing: ", rsync_call
        print "---"
        try:
            subprocess.check_call(rsync_call)
        except subprocess.CalledProcessError, e:
            raise e

    def _list_previous_dests(self):
        info_str = self._run_remotely("{} info {}".format(self.rsnap2, self.remote_root))

        previous_dests = []
        for info_line in info_str.splitlines():
            previous_dests.append(RsyncBackuperDest.parse(info_line, self.remote_root))

        return previous_dests

    def _complete_dest(self, dest):
        assert dest.root == self.root

        self._run_remotely("{} __service {} mark-completed {}".format(self.rsnap2, self.remote_root, dest.dirname))

    def _run_remotely(self, cmd):
        cmd_call = self.rsh + [cmd]

        print "ISSUING REMOTE: ", cmd_call
        print "---"
        try:
            return subprocess.check_output(cmd_call)
        except subprocess.CalledProcessError, e:
            print >>sys.stderr, "REMOTE ERROR"
            raise e  # TODO: how to prevent check_output from forwarding stdout/stderr on error? use Popen?


if __name__ == "__main__":
    #
    # action definitions
    #
    def BackupAction(args):
        if True in (arg.startswith("--link-dest") for arg in args.rsync_args):
            raise ValueError("--link-dest cannot be overwritten in --rsync-args")

        backuper = RsyncBackuper.create(args.root, args.rsync_args, args.rsh, args.rsh_rsnap2)
        backuper.backup(args.sources)
        return 0

    def InfoAction(args):
        backuper = RsyncBackuper.create(args.root, args.rsync_args, args.rsh, args.rsh_rsnap2)

        dests = [dest.dirname for dest in backuper.dests]
        if len(dests) > 0:
            print "{}".format(os.linesep.join(dests))
        return 0

    def ServiceAction_MarkCompleted(args):
        backuper = RsyncBackuper.create(args.root, args.rsync_args, args.rsh, args.rsh_rsnap2)

        dest = RsyncBackuperDest.parse(args.dest, backuper.root)
        if dest is None:
            return 1
        backuper._complete_dest(dest)


    #
    # command line parser definitions
    #
    args_parser = argparse.ArgumentParser(description="rsync snapshot backups")
    actions_parsers = args_parser.add_subparsers()

    # user actions
    actions_useraction_parent = argparse.ArgumentParser(add_help=False)
    actions_useraction_parent.add_argument("--rsh")
    actions_useraction_parent.add_argument("--rsh-rsnap2")
    actions_useraction_parent.add_argument("--rsync-args", nargs=argparse.REMAINDER, default=["-a", "-v"])

    action_backup = actions_parsers.add_parser("backup", parents=[actions_useraction_parent])
    action_backup.add_argument("sources", nargs="+")
    action_backup.add_argument("root")
    action_backup.set_defaults(handler=BackupAction)

    action_info = actions_parsers.add_parser("info", parents=[actions_useraction_parent])
    action_info.add_argument("root")
    action_info.set_defaults(handler=InfoAction)

    # internal actions
    action_service = actions_parsers.add_parser("__service")
    action_service.add_argument("root")
    action_service_subparsers = action_service.add_subparsers()
    action_service_markcompleted = action_service_subparsers.add_parser("mark-completed", parents=[actions_useraction_parent])
    action_service_markcompleted.add_argument("dest")
    action_service_markcompleted.set_defaults(handler=ServiceAction_MarkCompleted)



    #
    # program execution
    #
    args = args_parser.parse_args()
    exit(args.handler(args))
