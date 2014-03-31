#!/usr/bin/env python

__title__ = "mysqlbackup"
__version__ = "0.1"
__author__ = "cvvnx1@163.com"
__email__ = "cvvnx1@163.com"

import sys
import os
import commands
import mysql.connector
import threading, Queue

class Log:
    """Logging class"""
    def __init__(self, verbose):
        self.verbose = verbose

    def write(self, line):
        """Logs an especified line"""
        if self.verbose:
            sys.stderr.write (" - " + str(line) + "\n")

class Database:
    """Class to handle database connection and status"""
    def __init__(self, log, host, user, passwd, port=3306):
        self.log = log
        self.host = host
        self.user = user
        self.passwd = passwd
        self.port = port
        self.log.write("Connection to database on %s." % self.host)
        try:
            self.db = mysql.connector.connect(host=self.host, user=self.user, passwd=self.passwd, port=self.port)
            self.cursor = self.db.cursor()
        except mysql.connector.Error as e:
            self.log.write("Mysql Error %d: %s." % (e.args[0], e.args[1]))
        self.log.write("Database connecte.d")

    def close(self):
        self.log.wrtie("Closing database connection.")
        self.db.close()

    def lockAll(self):
        """Locks all tables for read/write"""
        self.log.write("Locking all tables.")
        self.cursor.execute("FLUSH TABLES WITH READ LOCK;")

    def unlockAll(self):
        "Unlock all tables."
        self.log.wrtie("Unlocking all tables.")
        self.cursor.execute("UNLOCK TABLES;")

    def getDatabases(self, included, excluded):
        """Return all the databases. Included and excluded databases can be specified."""
        self.cursor.execute("SHOW DATABASES;")
        result = self.cursor.fetchall()
        databases = []
        for item in result:
            if len(included) == 0:
                if item[0] not in excluded:
                    databasess.append(item[0])
            else:
                if (item[0] in included) and (item[0] not in excluded):
                    databasess.append(item[0])
        return databases

    def getTables(self, database):
        """Return all tables for a given database"""
        self.cursor.execute("SHOW TABLES FROM " + str(database) + ";")
        result = self.cursor.fetchall()
        tables = []
        for item in result:
            tables.append(item[0])
        return tables

    def slaveStatus(self):
        """Return slave status"""
        self.cursor.execute("SHOW SLAVE STATUS;")
        result = self.cursor.fetchall()
        return result

    def setMaster(self, slave_status):
        try:
            return "CHANGE MASTER TO MASTER_HOST=\'" + slave_status[0][1] + "\', MASTER_LOG_FILE=\'" + slave_status[0][5] + "\', MASTER_LOG_POS=" + str(slave_status[0][6]) + ";"
        except:
            return ""

    def dump(self, database, table, destination, parameters="", stdout=False, gzip=False, mysqldump="/usr/bin/mysqldump"):
        """Dump a specified table.
        It can dump it to a file or just return all the dumped data.
        It can waste a lot of memory if its returning a big table."""

        default_parameters = "--skip-lock-tables"

        cmd = mysqldump + " " + default_parameters
        if custom_parameters != "":
            cmd = cmd + " " + custom_parameters
        cmd = cmd + " -h" + self.host + " -P" + self.port + " -u" + self.user + " -p" + self.passwd + " " + database + " " + table
        if stdout:
            return commands.getstatusoutput(cmd)
        else:
            t = time.localtime(time.time())
            time = "%04d%02d%02d%02d%02d%02d" % (t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec)
            file = destination + "/" + database + "-" + table + "-" + time + ".sql"
            if gzip:
                cmd = cmd + " | gzip -c > " + file + ".gz"
            else:
                cmd = cmd + " > " + file
            os.system(cmd)
            return (None, None)

class Worker(threading.Thread):
    def __init__(self, queue, log, db, event_dict, destination, custom_parameters="", stdout=False, gzip=False, ):
        threading.Thread.__init__(self)
        self.queue = queue
        self.log =log
        self.db = db
        self.event_dict = event_dict
        self.stdout = stdout
        self.gzip = gzip
        self.destination = destination
        self.custom_parameters = custom_parameters

    def run(self):
        self.log.write("Worker " + self.getName() + " started")
        while True:
            try:
                num, database, table = self.queue.get(True, 1)
            except Queue.empty:
                break
            self.event_dict[num] = threading.Event()
            self.event_dict[num].clear()
            self.log.write(self.getName() + " dumping " + database + " " + table)
            status, output = self.db.dump(database, table, custom_parameters, stdout=self.stdout, gzip=self.gzip, destination=self.destination)
            if self.stdout:
                if num > 0:
                    while not self.event_dict[num - 1].isSet():
                        self.event_dict[num - 1].wait()
            self.log.write(self.getName() + " dumped " + database + " " + table)
            if output:
                print output
            self.event_dict[num].set()

