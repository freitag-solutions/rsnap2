#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# Yet Another Rsync SNAPshot utility (yarsnap).
# Copyright 2015 André Freitag <andre@freitag.solutions>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import argparse
import datetime
import operator
import os
import re
import shlex
import subprocess
import sys


class RsyncBackuper(object):
    def __init__(self, root, host, rsync_args):
        self.root = root
        self.host = host
        self.rsync_args = rsync_args

        dests = self._list_previous_dests()
        dests.sort(key=operator.attrgetter("time"), reverse=True)

        self.dests = dests

    def backup(self, sources):
        dest = RsyncBackuperDest.new(self.root, self.host)
        previous_backup = None
        if len(self.dests) > 0:
            previous_backup = next((x for x in self.dests if x.is_complete), None)

        rsync_params = sources + [dest.hostPath]
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
    def create(cls, root, rsync_args, rsh, rsh_yarsnap):
        if ":" in root:
            # username@remote_host:path
            tmp = root.split(":")
            if len(tmp) != 2:
                raise Exception("illegal use of : in the root path: %s" % root)

            remote_host_string = tmp[0]
            remote_root = tmp[1]

            tmp = remote_host_string.split("@")
            if len(tmp) == 1:
                host = tmp[0], None
            elif len(tmp) == 2:
                host = tmp[1], tmp[0]
            else:
                raise Exception("illegal use of @ in the root path: %s" % root)

            if rsh is None:
                raise Exception("--rsh must be given when targeting a remote repository")

            return RsyncBackuper_Remote(remote_root, host, rsh, rsh_yarsnap, rsync_args)
        else:
            root = os.path.abspath(root)
            if not os.path.exists(root) or not os.path.isdir(root):
                raise ValueError("no such dir: {0}".format(root))

            return RsyncBackuper_Local(root, rsync_args)


class RsyncBackuperDest:
    DEST_DIR_FORMAT = "{time}{dotflag}"
    DEST_DIR_DATE_FORMAT = "%Y-%m-%d_%H-%M-%S.%f"
    DEST_DIR_RE = re.compile(r"^(?P<time>\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}.\d{6})(?P<flag>\.partial)?$")

    def __init__(self, dirname, root, host, time, is_complete):
        self.dirname = dirname
        self.root = root
        self.host = host
        self.time = time
        self.is_complete = is_complete

    @property
    def path(self):
        return os.path.join(self.root, self.dirname)

    @property
    def hostPath(self):
        if self.host is None:
            return self.path
        else:
            if self.host[1] is not None:
                host = "%s@%s" % (self.host[1], self.host[0])
            else:
                host = self.host[0]
            return os.path.join("{}:{}".format(host, self.root), self.dirname)

    @classmethod
    def parse(cls, dirname, root, host):
        match = cls.DEST_DIR_RE.match(dirname)
        if match is None:
            return None

        time = datetime.datetime.strptime(match.group("time"), cls.DEST_DIR_DATE_FORMAT)

        flag = match.group("flag")
        is_complete = flag is None

        return cls(dirname=dirname, root=root, host=host, time=time, is_complete=is_complete)

    @classmethod
    def new(cls, root, host):
        time = datetime.datetime.now()
        dirname = cls._get_dirname(time, "partial")

        return cls(dirname=dirname, root=root, host=host, time=time, is_complete=False)

    @classmethod
    def _get_dirname(cls, time, flag=None):
        return cls.DEST_DIR_FORMAT.format(
            time=time.strftime(cls.DEST_DIR_DATE_FORMAT),
            dotflag="" if flag is None else ".{}".format(flag)
        )


class RsyncBackuper_Local(RsyncBackuper):
    def __init__(self, root, rsync_args):
        super(RsyncBackuper_Local, self).__init__(root, None, rsync_args)

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
            dest = RsyncBackuperDest.parse(root_subdir, self.root, self.host)

            if dest is not None:
                previous_dests.append(dest)

        return previous_dests

    def _complete_dest(self, dest):
        assert dest.root == self.root and dest.host == self.host

        if not(os.path.exists(dest.path) and os.path.isdir(dest.path)):
            raise Exception("destination doesn't exist! maybe check your rsync arguments?")

        path_old = dest.path

        dest.is_complete = True
        dest.dirname = dest._get_dirname(dest.time)
        path_new = dest.path

        os.rename(path_old, path_new)


class RsyncBackuper_Remote(RsyncBackuper):
    def __init__(self, root, host, rsh, rsh_yarsnap, rsync_args):
        self.yarsnap = "yarsnap" if rsh_yarsnap is None else rsh_yarsnap

        self.rsh_orig = rsh
        rsh = shlex.split(rsh)
        if host[1] is not None:
            rsh += ["%s@%s" % (host[1], host[0])]
        else:
            rsh += [host[0]]
        self.rsh = rsh

        super(RsyncBackuper_Remote, self).__init__(root, host, rsync_args)

    def _issue_rsync(self, params):
        rsync_call = ["rsync"] + ["--rsh", self.rsh_orig] + params

        print "issuing: ", rsync_call
        print "---"
        try:
            subprocess.check_call(rsync_call)
        except subprocess.CalledProcessError, e:
            raise e

    def _list_previous_dests(self):
        info_str = self._run_remotely([self.yarsnap, "info", self.root])

        previous_dests = []
        for info_line in info_str.splitlines():
            previous_dests.append(RsyncBackuperDest.parse(info_line, self.root, self.host))

        return previous_dests

    def _complete_dest(self, dest):
        assert dest.root == self.root and dest.host == self.host

        self._run_remotely([self.yarsnap, "__service", self.root, "mark-completed", dest.dirname])

    def _run_remotely(self, cmd):
        from pipes import quote as shell_quote
        cmd_call = self.rsh + [" ".join([shell_quote(c) for c in cmd])]

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

        backuper = RsyncBackuper.create(args.root, args.rsync_args, args.rsh, args.rsh_yarsnap)
        backuper.backup(args.sources)
        return 0

    def InfoAction(args):
        backuper = RsyncBackuper.create(args.root, args.rsync_args, args.rsh, args.rsh_yarsnap)

        dests = [dest.dirname for dest in backuper.dests]
        if len(dests) > 0:
            print "{}".format(os.linesep.join(dests))
        return 0

    def ServiceAction_MarkCompleted(args):
        backuper = RsyncBackuper.create(args.root, None, None, None)

        dest = RsyncBackuperDest.parse(args.dest, backuper.root, backuper.host)
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
    actions_useraction_parent.add_argument("--rsh-yarsnap")
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
    action_service_markcompleted = action_service_subparsers.add_parser("mark-completed")
    action_service_markcompleted.add_argument("dest")
    action_service_markcompleted.set_defaults(handler=ServiceAction_MarkCompleted)



    #
    # program execution
    #
    args = args_parser.parse_args()
    exit(args.handler(args))
