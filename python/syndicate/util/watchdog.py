#!/usr/bin/env python

"""
   Copyright 2014 The Trustees of Princeton University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

# utility functions for making (gateway) watchdogs

import sys
import os
import grp
import pwd
import signal
import errno
import time
import logging
import psutil

logging.basicConfig(format='[%(levelname)s] [%(module)s:%(lineno)d] %(message)s')

log = logging.getLogger()


#-------------------------------
def is_attr_str(attr_str):
    return attr_str.startswith("attr:") and "=" in attr_str


#-------------------------------
def attr_proc_title(binary, attrs):
    """
    Make a process title that has attr:<k>=<v> for set of attributes.
    """
    return "%s %s" % (binary, " ".join(["attr:%s=%s" % (k, v) for (k, v) in attrs.items()]))


#-------------------------------
def parse_proc_attrs(proc_cmdline):
    """
    Given a process command-line, find out its attrs.
    """
    attrs = {}

    # format: <binary> <attr:k=v>...
    parts = proc_cmdline.split(" ")

    attr_list = parts[1:]

    for attr_kv in attr_list:

        if is_attr_str(attr_kv):
            kv = attr_kv[len("atttr:")]
            kv_parts = kv.split("=")

            key = kv_parts[0]
            value = "=".join(kv_parts[1:])

            attrs[key] = value

    return attrs


#-------------------------------
def get_proc_pid(p):
    return p.pid


#-------------------------------
def get_proc_cmdline(p):
    # depending on which version of psutil we're using, this is either a list
    # or a callable that evaluates to a list.
    cmdline = None

    try:
        cmdline = p.cmdline()
    except Exception:
        cmdline = p.cmdline

    assert type(cmdline) == list, "Unable to iterate over process command-lines"

    return cmdline


#-------------------------------
def find_by_attrs(watchdog_name, attrs):
    """
    Find a running watchdog by a given attribute set
    NOTE: it must have a process cmdline generated by attr_proc_title()
    """
    procs = []
    for p in psutil.process_iter():

        cmdline = get_proc_cmdline(p)

        if len(cmdline) == 0:
            continue

        parts = cmdline[0].split(" ")
        if len(parts) < 2:
            continue

        name = parts[0]
        attr_list = parts[1:]

        if name == watchdog_name:
            # switch on attrs
            matching_attrs = []
            match = True

            # find matching attrs
            for attr_kv in attr_list:

                if is_attr_str(attr_kv):
                    # parse this: attr:<name>=<value>
                    kv = attr_kv[len("attr:"):]

                    kv_parts = kv.split("=")
                    key = kv_parts[0]
                    value = "=".join(kv_parts[1:])

                    if attrs.has_key(key) and attrs[key] == value:
                        matching_attrs.append(key)

            # did we match all attrs?
            for attr_name in attrs.keys():
                if attr_name not in matching_attrs:
                    match = False
                    break

            if match:
                procs.append(p)

    return procs


#-------------------------------
def release_privileges(uid_name, gid_name):

    if os.getuid() != 0:
        # not root, so nothing to do
        return 0

    # get desired uid/gid
    try:
        running_pwd = pwd.getpwnam(uid_name)
        running_uid = running_pwd.pw_uid
    except KeyError:
        log.error("No such user '%s'" % uid_name)
        return -errno.EINVAL

    try:
        running_grp = grp.getgrnam(gid_name)
        running_gid = running_grp.gr_gid
    except KeyError:
        log.error("No such group '%s'" % gid_name)
        return -errno.EINVAL

    # unset groups
    os.setgroups([])

    # set new uid/gid
    os.setgid(running_gid)
    os.setuid(running_uid)

    return 0


#-------------------------------
def spawn(child_method, stdin_method, old_exit_status, parent_signals=None, uid_name=None, gid_name=None):
    # get stdin
    stdin_buf = None
    child_stdin = -1
    parent_w = -1

    if stdin_method is not None:
        stdin_buf = stdin_method()

        # create child stdin
        try:
            child_stdin, parent_w = os.pipe()
        except OSError, oe:
            print >> sys.stderr, "pipe errno = %s" % oe.errno
            return -oe.errno

    # fork, become a watchdog, and run the main server
    try:
        child_pid = os.fork()
    except OSError, oe:
        # failure
        log.exception(oe)
        log.error("Failed to fork, errno = %s" % oe.errno)

    if child_pid > 0:
        # we're the parent

        if parent_w != -1 and child_stdin != -1:
            # send stdin
            os.close(child_stdin)

            # parent.  Send stdin
            os.write(parent_w, stdin_buf)
            os.close(parent_w)

        log.info("Spawned child %s" % child_pid)

        return child_pid

    else:
        # we're the child...
        # set default signal handlers--don't mess with the parent's
        if parent_signals is not None:
            for sig in parent_signals:
                signal.signal(sig, signal.SIG_DFL)

        if parent_w != -1 and child_stdin != -1:
            os.close(parent_w)

            sys.stdout.flush()
            sys.stderr.flush()

            # redirect stdin
            os.dup2(child_stdin, sys.stdin.fileno())
            os.close(child_stdin)

        # drop privileges, if we have to
        if uid_name is not None and gid_name is not None:
            release_privileges(uid_name, gid_name)

        rc = child_method(old_exit_status)
        sys.exit(rc)


#-------------------------------
def check_exit_status(exit_status, respawn_exit_statuses=None, respawn_signals=None):
    # determine whether or not we need to respawn, based on the exit and signal status.

    need_respawn = False

    # did the child exit on its own accord?
    if os.WIFEXITED(exit_status):
        ex_status = os.WEXITSTATUS(exit_status)
        log.info("Child exit status %s" % ex_status)

        if respawn_exit_statuses is None or ex_status in respawn_exit_statuses:
            need_respawn = True

    # was the child signaled?
    elif os.WIFSIGNALED(exit_status):
        sig = os.WTERMSIG(exit_status)
        log.info("Child exited with signal %s" % sig)

        if respawn_signals is None or sig in respawn_signals:
            need_respawn = True

    return need_respawn


#-------------------------------
def kill_child(child_pid):
    # send SIGKILL
    log.info("Sending SIGKILL to %s" % child_pid)
    os.kill(child_pid, signal.SIGKILL)


#-------------------------------
def stop_child(child_pid):
    # send SIGTERM, then SIGKILL
    log.info("Sending SIGTERM to %s" % child_pid)
    os.kill(child_pid, signal.SIGTERM)

    count = 3
    while count > 0:
        try:
            pid, exit_status = os.waitpid(child_pid, os.WNOHANG)
        except OSError, e:
            log.exception(e)

            # if this wasn't ECHILD (i.e. the PID isn't invalid), then kill the process and die
            if e.errno != errno.ECHILD:
                kill_child(child_pid)
                return -1

        # did it exit?
        if os.WIFEXITED(exit_status) or os.WIFSIGNALED(exit_status):
            log.info("Child %s exited with status %s" % (child_pid, exit_status))
            break

        # if not, try again
        time.sleep(1.0)
        count -= 1

    if count == 0:
        kill_child(child_pid)

    return 0


#-------------------------------
def flap_wait(last_spawn, flap_delay, flap_threshold, flap_reset):
    # wait before spawning another process, to avoid flapping.
    # return the new flap delay.
    t = time.time()
    if t - last_spawn > flap_reset:
        flap_delay = 1

    if t - last_spawn < flap_threshold:
        log.warning("Child is respawning too quickly.  Waiting %s seconds to try again" % flap_delay)

        time.sleep(flap_delay)

        flap_delay *= 2

        if flap_delay > flap_reset:
            flap_delay = flap_reset

    return flap_delay


#-------------------------------
def run(binary, argv, stdin_buf, uid_name=None, gid_name=None):
    """
    Fork a child and feed it a stdin.
    Return its pid on success
    return errno on error
    """

    def do_exec(ignored):
        # exec the binary
        try:
            os.execvp(binary, argv)
        except OSError, oe:
            log.error("Failed to execute %s, errno = %s" % (binary, -oe.errno))
            return -oe.errno


    stdin_method = lambda: stdin_buf

    return spawn(do_exec, stdin_method, 0, uid_name=uid_name, gid_name=gid_name)


#-------------------------------
def main(spawn_method, pid_cb=None, stdin_cb=None, exit_cb=None, respawn_exit_statuses=None, respawn_signals=None, parent_signals=None, flap_threshold=600, flap_reset=3600, uid_name=None, gid_name=None):

    # fork, become a watchdog, and run the main server
    last_spawn = time.time()
    flap_delay = 1

    # but first, drop privileges if we need to
    if uid_name is not None and gid_name is not None:
        release_privileges(uid_name, gid_name)

    child_pid = spawn(spawn_method, stdin_cb, 0, parent_signals=parent_signals, uid_name=uid_name, gid_name=gid_name)

    if child_pid < 0:
        # spawn failed
        log.error("Failed to spawn, rc = %s" % child_pid)
        return child_pid

    if pid_cb is not None:
        # pass along the PID
        pid_cb(child_pid)

    # we're the parent...start monitoring
    while True:

        # wait for child to die
        try:
            pid, exit_status = os.waitpid(child_pid, 0)
        except OSError, e:
            log.exception(e)
            log.error("PID %s exiting" % os.getpid())
            sys.exit(1)

        except (KeyboardInterrupt, SystemExit):
            # kill the child and return
            my_exit_status = stop_child(child_pid)
            log.info("watchdog exit, child exit status %s" % my_exit_status)
            return my_exit_status

        need_respawn = False

        # find out if we need to respawn
        if exit_cb is not None:
            need_respawn = exit_cb(exit_status)
        else:
            need_respawn = check_exit_status(exit_status, respawn_exit_statuses, respawn_signals)

        if need_respawn:
            # are we flapping?  Wait for a bit if so
            if flap_threshold > 0:
                flap_delay = flap_wait(last_spawn, flap_delay, flap_threshold, flap_reset)

            child_pid = spawn(spawn_method, stdin_cb, exit_status, parent_signals=parent_signals)

            if child_pid < 0:
                # spawn failed
                log.error("Failed to spawn, rc = %s" % child_pid)
                return child_pid

            # pass along the PID
            if pid_cb is not None:
                pid_cb(child_pid)

            last_spawn = time.time()

        else:
            # no respawn...return the exit status
            log.info("not respawning, child exit status %s" % exit_status)
            return exit_status
