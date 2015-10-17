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
import logging
import operator
import os
import re
import shlex
import subprocess
import sys

from pipes import quote as shell_quote


class YarsnapBackuper(object):
    def __init__(self, repository, rsync_args):
        self.repository = repository
        self.rsync_args = rsync_args

        dests = self.repository.list_snapshots()
        dests.sort(key=operator.attrgetter("time"), reverse=True)
        self.dests = dests

        self.logger = logging.getLogger(self.__class__.__name__)

    def backup(self, sources):
        dest = Snapshot.new(self.repository)
        previous_backup = None
        if len(self.dests) > 0:
            previous_backup = next((x for x in self.dests if x.is_complete), None)

        rsync_params = sources + [dest.hostPath]
        if previous_backup is not None:
            rsync_params += ["--link-dest", previous_backup.path]
        rsync_params += self.rsync_args

        self._issue_rsync(rsync_params)

        self.repository.complete_dest(dest)

    def _issue_rsync(self, params):
        rsync_call = ["rsync"] + params

        self.logger.info("issuing rsync: %s", " ".join([shell_quote(s) for s in rsync_call]))

        print >>sys.stderr, ""
        ret = subprocess.call(rsync_call, stdout=sys.stderr, stderr=sys.stderr)
        print >>sys.stderr, ""

        if ret != 0:
            raise Exception("rsync failed")


class SnapshotRepository(object):
    def __init__(self, root, host, rsh, rsh_yarsnap):
        self.root = root
        self.host = host
        self.rsh = rsh
        self.rsh_yarsnap = "yarsnap" if rsh_yarsnap is None else rsh_yarsnap

        self.logger = logging.getLogger(self.__class__.__name__)

    def list_snapshots(self):
        raise NotImplementedError()

    def complete_dest(self, dest):
        raise NotImplementedError()

    @classmethod
    def create(cls, root, host, rsh, rsh_yarsnap):
        if host is None:
            return LocalSnapshotRepository(root=root, host=host, rsh=rsh, rsh_yarsnap=rsh_yarsnap)
        else:
            return RemoteSnapshotRepository(root=root, host=host, rsh=rsh, rsh_yarsnap=rsh_yarsnap)


class LocalSnapshotRepository(SnapshotRepository):
    def __init__(self, root, host, rsh, rsh_yarsnap):
        assert os.path.isabs(root)
        assert os.path.isdir(root)
        super(LocalSnapshotRepository, self).__init__(root, host, rsh, rsh_yarsnap)

    def list_snapshots(self):
        previous_dests = []

        root_subdirs = [child for child in os.listdir(self.root) if os.path.isdir(os.path.join(self.root, child))]
        for root_subdir in root_subdirs:
            dest = Snapshot.existing(root_subdir, self)

            if dest is not None:
                previous_dests.append(dest)

        return previous_dests

    def complete_dest(self, dest):
        assert dest.repository == self

        if not(os.path.exists(dest.path) and os.path.isdir(dest.path)):
            raise Exception("destination doesn't exist! maybe check your rsync arguments?")

        path_old = dest.path

        dest.is_complete = True
        dest.dirname = dest._get_dirname(dest.time)
        path_new = dest.path

        os.rename(path_old, path_new)


class RemoteSnapshotRepository(SnapshotRepository):
    def __init__(self, root, host, rsh, rsh_yarsnap):
        assert rsh is not None
        super(RemoteSnapshotRepository, self).__init__(root, host, rsh, rsh_yarsnap)

    def list_snapshots(self):
        info_str = self._remote_yarsnap(["info", self.root])

        previous_dests = []
        for info_line in info_str.splitlines():
            previous_dests.append(Snapshot.existing(info_line, self))

        return previous_dests

    def complete_dest(self, dest):
        assert dest.repository == self

        self._remote_yarsnap(["__service", self.root, "mark-completed", dest.dirname])

    def _remote_yarsnap(self, cmd):
        if "args" in globals() and hasattr(globals()["args"], "verbosity") and globals()["args"].verbosity > 0:
            cmd += ["-"+"v"*globals()["args"].verbosity]

        cmd_call = shlex.split(self.rsh)
        if self.host[1] is not None:
            cmd_call += ["%s@%s" % (self.host[1], self.host[0])]
        else:
            cmd_call += [self.host[0]]
        cmd_call += [self.rsh_yarsnap]
        cmd_call += [" ".join([shell_quote(c) for c in cmd])]

        self.logger.info("issuing remote yarsnap: %s", " ".join([shell_quote(c) for c in cmd_call]))
        try:
            return subprocess.check_output(cmd_call, stderr=sys.stderr)
        except subprocess.CalledProcessError, e:
            raise e


class Snapshot(object):
    DEST_DIR_FORMAT = "{time}{dotflag}"
    DEST_DIR_DATE_FORMAT = "%Y-%m-%d_%H-%M-%S.%f"
    DEST_DIR_RE = re.compile(r"^(?P<time>\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}.\d{6})(?P<flag>\.partial)?$")

    def __init__(self, repository, dirname, time, is_complete):
        assert repository is not None

        self.repository = repository
        self.dirname = dirname
        self.time = time
        self.is_complete = is_complete

        self.logger = logging.getLogger(self.__class__.__name__)

    @property
    def path(self):
        return os.path.join(self.repository.root, self.dirname)

    @property
    def hostPath(self):
        if self.repository.host is None:
            return self.path
        else:
            if self.repository.host[1] is not None:
                host = "%s@%s" % (self.repository.host[1], self.repository.host[0])
            else:
                host = self.repository.host[0]
            return os.path.join("{}:{}".format(host, self.repository.root), self.dirname)

    @classmethod
    def new(cls, repository):
        time = datetime.datetime.now()
        dirname = cls._get_dirname(time, "partial")

        return cls(repository=repository, dirname=dirname, time=time, is_complete=False)

    @classmethod
    def existing(cls, dirname, repository):
        match = cls.DEST_DIR_RE.match(dirname)
        if match is None:
            return None

        time = datetime.datetime.strptime(match.group("time"), cls.DEST_DIR_DATE_FORMAT)

        flag = match.group("flag")
        is_complete = flag is None

        return cls(repository=repository, dirname=dirname, time=time, is_complete=is_complete)

    @classmethod
    def _get_dirname(cls, time, flag=None):
        return cls.DEST_DIR_FORMAT.format(
            time=time.strftime(cls.DEST_DIR_DATE_FORMAT),
            dotflag="" if flag is None else ".{}".format(flag)
        )


if __name__ == "__main__":
    #
    # action definitions
    #
    def BackupAction(args):
        logging.debug("handling: BackupAction")
        backuper = backuper_from_args(arg_root=args.root, arg_rsh=args.rsh, arg_rsh_yarsnap=args.rsh_yarsnap, arg_rsync_args=args.rsync_args)

        backuper.backup(args.sources)
        return 0

    def InfoAction(args):
        logging.debug("handling: InfoAction")
        backuper = backuper_from_args(arg_root=args.root, arg_rsh=args.rsh, arg_rsh_yarsnap=args.rsh_yarsnap, arg_rsync_args=args.rsync_args)

        dests = [dest.dirname for dest in backuper.dests]
        if len(dests) > 0:
            print "{}".format(os.linesep.join(dests))
        return 0

    def ServiceAction_MarkCompleted(args):
        logging.debug("handling: ServiceAction_MarkCompleted")
        repository = repository_from_args_for_service(arg_root=args.root)

        dest = Snapshot.existing(args.dest, repository)
        if dest is None:
            return 1
        repository.complete_dest(dest)
        return 0


    #
    # command line parser definitions
    #
    args_parser = argparse.ArgumentParser(description="rsync snapshot backups")

    actions_parsers = args_parser.add_subparsers(dest="action")

    actions_parent = argparse.ArgumentParser(add_help=False)
    actions_parent.add_argument("-v", "--verbosity", action="count", default=0)

    # user actions
    actions_useraction_parent = argparse.ArgumentParser(add_help=False)
    actions_useraction_parent.add_argument("--rsh")
    actions_useraction_parent.add_argument("--rsh-yarsnap")
    actions_useraction_parent.add_argument("--rsync-args", nargs=argparse.REMAINDER, default=["-a", "-v"])

    action_backup = actions_parsers.add_parser("backup", parents=[actions_parent, actions_useraction_parent])
    action_backup.add_argument("sources", nargs="+")
    action_backup.add_argument("root")
    action_backup.set_defaults(handler=BackupAction)

    action_info = actions_parsers.add_parser("info", parents=[actions_parent, actions_useraction_parent])
    action_info.add_argument("root")
    action_info.set_defaults(handler=InfoAction)

    # service actions
    action_service = actions_parsers.add_parser("__service")
    action_service.add_argument("root")

    action_service_subparsers = action_service.add_subparsers()
    action_service_markcompleted = action_service_subparsers.add_parser("mark-completed", parents=[actions_parent])
    action_service_markcompleted.add_argument("dest")
    action_service_markcompleted.set_defaults(handler=ServiceAction_MarkCompleted)


    #
    # helpers
    #
    def backuper_from_args(arg_root, arg_rsh, arg_rsh_yarsnap, arg_rsync_args):
        repository = repository_from_args(arg_root=arg_root, arg_rsh=arg_rsh, arg_rsh_yarsnap=arg_rsh_yarsnap)

        return YarsnapBackuper(repository, arg_rsync_args + (["--rsh", arg_rsh] if arg_rsh is not None else []))

    def repository_from_args(arg_root, arg_rsh, arg_rsh_yarsnap):
        host = None
        if ":" in arg_root:
            # username@remote_host:path
            tmp = arg_root.split(":")
            if len(tmp) != 2:
                raise Exception("illegal use of : in the root path: %s" % arg_root)

            remote_host_string = tmp[0]
            root = tmp[1]

            tmp = remote_host_string.split("@")
            if len(tmp) == 1:
                host = tmp[0], None
            elif len(tmp) == 2:
                host = tmp[1], tmp[0]
            else:
                raise Exception("illegal use of @ in the root path: %s" % root)

            if arg_rsh is None:
                raise Exception("--rsh must be given when targeting a remote repository")
        else:
            root = os.path.abspath(arg_root)

        return SnapshotRepository.create(root=root, host=host, rsh=arg_rsh, rsh_yarsnap=arg_rsh_yarsnap)

    def repository_from_args_for_service(arg_root):
        root = os.path.abspath(arg_root)

        return SnapshotRepository.create(root=root, host=None, rsh=None, rsh_yarsnap=None)


    #
    # program execution
    #
    args = args_parser.parse_args()

    # set up logging
    if args.verbosity > 2:
        args_parser.error("--verbosity cannot not exceed 2")

    log_level = {
        0: logging.WARNING,
        1: logging.INFO,
        2: logging.DEBUG
    }[args.verbosity]

    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    if args.action == "__service":
        log_format = "%(asctime)s - __service:%(name)s - %(levelname)s - %(message)s"

    logging.basicConfig(level=log_level, format=log_format)

    # delegate to action handler
    try:
        ret = args.handler(args)
    except:
        logging.exception("exception, run with -vv for more info")
        ret = -1
    exit(ret)
