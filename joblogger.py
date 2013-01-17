#! /usr/bin/env python
##############################################################################
#
# Copyright (c) 2013 Brandscreen Pty Ltd.
# All Rights Reserved.
#
# This software is subject to the provisions of the BSD-like license at
# http://www.repoze.org/LICENSE.txt.  A copy of the license should accompany
# this distribution.  THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL
# EXPRESS OR IMPLIED WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO,
# THE IMPLIED WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND
# FITNESS FOR A PARTICULAR PURPOSE
#
##############################################################################

# A event listener meant to be subscribed to PROCESS_STATE_CHANGE
# events.  It will log when processes that are children of supervisord
# transition state.

# A supervisor config snippet that tells supervisor to use this script
# as a listener is below.
#
# [eventlistener:joblogger]
# command=joblogger.py
# events=PROCESS_STATE
# dbpath=sqlite:////var/lib/joblogger.db

#
# usage:
# joblogger.py - runs the job logger from supervisord
# joblogger.py list - lists the status of all jobs
# joblogger.py check GROUP PROCESS TIMEDELTA - checks status of the PROCESS_STATE

import os
import sys
import glob
import time
import json
import signal
import logging
import datetime
import ConfigParser
from uuid import uuid4
from threading import Timer
from supervisor import childutils
from collections import defaultdict
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.expression import func, and_
from sqlalchemy.orm import sessionmaker


CONFIG_FILE = '/etc/supervisor/supervisord.conf'
DEFAULT_DB = 'sqlite:////var/lib/joblogger.db'


Base = declarative_base()
Session = sessionmaker()


class Event(Base):
    """
    An event record, noting a start, finish or fail
    """
    __tablename__ = 'events'

    id = Column(String, primary_key=True)
    ts = Column(Integer)
    groupname = Column(String)
    processname = Column(String)
    eventname = Column(String)
    pid = Column(Integer)

    def __repr__(self):
        return "Event(id={0}, ts={1}, groupname={2}, processname={3}, eventname={4}, pid={5})".format(self.id, self.ts, self.groupname, self.processname, self.eventname, self.pid)


class JobLogger:
    """
    The job logger class that handles the main execution of the program.
    """
    def __init__(self, programs):
        self.stdin = sys.stdin
        self.stdout = sys.stdout
        self.stderr = sys.stderr
        self.programs = programs

    def run_forever(self):
        while True:
            # we explicitly use self.stdin, self.stdout, and self.stderr
            # instead of sys.* so we can unit test this code
            headers, payload = childutils.listener.wait(self.stdin, self.stdout)

            if headers['eventname'] in ['PROCESS_STATE_RUNNING', 'PROCESS_STATE_EXITED']:
                pheaders, pdata = childutils.eventdata(payload + '\n')

                pid = int(pheaders['pid'])

                event = Event(id=str(uuid4()), ts=int(time.time()), pid=pid,
                              groupname=pheaders['groupname'],
                              processname=pheaders['processname'])

                if headers['eventname'] == 'PROCESS_STATE_RUNNING':
                    event.eventname = 'STARTED'

                    # start a thread to kill the process if there is a max runtime
                    if pheaders['processname'] in self.programs:
                        timer = Timer(self.programs[pheaders['processname']].total_seconds(), os.kill, [pid, signal.SIGTERM])
                        timer.start()

                elif int(pheaders['expected']):
                    event.eventname = 'FINISHED'
                else:
                    event.eventname = 'FAILED'

                session = Session()
                session.add(event)
                session.commit()

            childutils.listener.ok(self.stdout)
            sys.stderr.flush()


def parse_program_runtimes(filename, programs):
    """
    Parse the program options for maxruntime settings
    """
    config = ConfigParser.SafeConfigParser()
    config.read(filename)

    for section in config.sections():
        if section == 'include':
            files = config.get('include', 'files')
            if files:
                for file_pattern in files.split():
                    for filenm in glob.iglob(file_pattern):
                        parse_program_runtimes(filenm, programs)
        else:
            section_parts = section.split(':')
            if section_parts[0] == 'program' and config.has_option(section, 'maxruntime'):
                programs[section_parts[1]] = convert_to_timedelta(config.get(section, 'maxruntime'))
                logging.info("{0} maxruntime {1}".format(section_parts[1], programs[section_parts[1]]))


def convert_to_timedelta(time_str):
    num = int(time_str[:-1])
    if time_str.endswith('s'):
        return datetime.timedelta(seconds=num)
    elif time_str.endswith('m'):
        return datetime.timedelta(minutes=num)
    elif time_str.endswith('h'):
        return datetime.timedelta(hours=num)
    elif time_str.endswith('d'):
        return datetime.timedelta(days=num)


def main():
    FORMAT = '%(asctime)-15s %(levelname)4s [%(process)d] %(message)s'
    logging.basicConfig(format=FORMAT)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

    logging.info("Starting JobLogger")

    # Ensure we're running under supervisord
    if not 'SUPERVISOR_SERVER_URL' in os.environ:
        logging.critical('joblogger must be run as a supervisor event '
                         'listener\n')
        sys.stderr.flush()
        return

    # Get the main config
    logging.info("Reading configuration")
    config = ConfigParser.SafeConfigParser({'dbpath': DEFAULT_DB})

    config.read(CONFIG_FILE)
    dbpath = config.get('eventlistener:joblogger', 'dbpath')

    # Parse the supervisord config file for maxruntimes
    programs = {}
    parse_program_runtimes(CONFIG_FILE, programs)

    # Create the job history database
    logging.info("Creating job history database")
    engine = create_engine(dbpath, echo=False)
    Base.metadata.create_all(engine)
    Session.configure(bind=engine)

    logging.info("Starting event listener")
    prog = JobLogger(programs)
    prog.run_forever()
    logging.info("Exiting")


def check_main(groupname=None, processname=None, maxtime=None):
    config = ConfigParser.SafeConfigParser({'dbpath': DEFAULT_DB})
    config.read(CONFIG_FILE)
    dbpath = config.get('eventlistener:joblogger', 'dbpath')

    engine = create_engine(dbpath, echo=False)
    Base.metadata.create_all(engine)
    Session.configure(bind=engine)

    session = Session()
    t = session.query(
	    Event.groupname,
	    Event.processname,
        func.max(Event.ts).label('ts'),
    ).group_by(Event.groupname, Event.processname).subquery('t')

    query = session.query(Event).filter(and_(
	    Event.groupname == t.c.groupname,
	    Event.processname == t.c.processname,
	    Event.ts == t.c.ts
    ))

    if groupname and processname:
        earliest = datetime.datetime.now() - convert_to_timedelta(maxtime)
        earliest = int(time.mktime(earliest.timetuple()))

        for event in query.filter_by(groupname=groupname, processname=processname):
            if event.ts < earliest:
                print "JOB CRITICAL: {0}/{1} last executed at {2}".format(event.groupname, event.processname, datetime.datetime.fromtimestamp(event.ts))
                exit(2)
            elif event.eventname == 'FAILED':
                print "JOB CRITICAL: {0}/{1} (pid {2}) failed at {3}".format(event.groupname, event.processname, event.pid, datetime.datetime.fromtimestamp(event.ts))
                exit(2)
            elif event.eventname == 'FINISHED':
                print "JOB OK: {0}/{1} (pid {2}) succeeded at {3}".format(event.groupname, event.processname, event.pid, datetime.datetime.fromtimestamp(event.ts))
                exit(0)
            elif event.eventname == 'STARTED':
                print "JOB OK: {0}/{1} (pid {2}) started at {3}".format(event.groupname, event.processname, event.pid, datetime.datetime.fromtimestamp(event.ts))
                exit(0)

        print "JOB CRITICAL: {0}/{1} not executed".format(groupname, processname)
        exit(2)
    else:
        events = defaultdict(dict)

        for event in query.order_by(Event.groupname):
            events[event.groupname][event.processname] = dict(status=event.eventname, ts=str(datetime.datetime.fromtimestamp(event.ts)))

        print json.dumps(events)


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'list':
        check_main()
    elif len(sys.argv) > 4 and sys.argv[1] == 'check':
        check_main(sys.argv[2], sys.argv[3], sys.argv[4])
    else:
        main()
