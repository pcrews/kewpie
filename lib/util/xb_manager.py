#! /usr/bin/env python
# -*- mode: python; indent-tabs-mode: nil; -*-
# vim:expandtab:shiftwidth=2:tabstop=2:smarttab:
#
# Copyright (C) 2011-2012 Patrick Crews, Valentine Gostev
#
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA

import os
import shutil
import re
import subprocess

""" Xtrabackup manager class handles all mysql backup operations
    it creates a backup object from server objects
"""

class xtrabackupManager:
    def __init__(self, server_manager, system_manager, variables):
        self.system_manager = system_manager
        self.code_manager = system_manager.code_manager
        self.server_manager = server_manager
        self.env_manager = system_manager.env_manager
        self.logging = system_manager.logging
        self.xb_bin_path = variables['xtrabackuppath']
        self.ib_bin_path = variables['innobackupexpath']
        self.backup_dir = os.path.join(variables['workdir'],'backups')
        if not os.path.isdir(self.backup_dir):
            os.makedirs(self.backup_dir)

    """ clean_dir method is used to wipe out data from server object's
        data directory in order to restore a prepared backup
        Usage clean_dir()
    """
    def clean_dir(self,path):
        for top,dirs,files in os.walk(path):
            for file in files:
                os.unlink(os.path.join(top,file))
            for dir in dirs:
                shutil.rmtree(os.path.join(top,dir))

    """ alloc_dir returns a directory name for next backup 
    """
    def alloc_dir(self, topdir, dir_pattern='backup'):
        dir_pattern_obj = '%s(\\d+)' %dir_pattern
        dir_pattern_obj = re.compile(dir_pattern_obj)
        dir_list = [list(dirs)[1] for dirs in os.walk(topdir) if list(dirs)[0] == topdir][0]
        dir_list = [b for b in dir_list if dir_pattern_obj.match(b)]
        if not dir_list:
            dir_suffix = '0'
        else:
            dir_suffix = str(max([int(re.split(dir_pattern,b)[1]) for b in dir_list])+1)

        return '%s%s' %(dir_pattern,dir_suffix)

    """ execute_cmd executes backup program to create backup object
    """
    def execute_cmd(self, cmd, exec_path, outfile_path):
        outfile = open(outfile_path,'w')
        cmd_subproc = subprocess.Popen( cmd
                                      , cwd = exec_path
                                      , shell=True
                                      , stdout = outfile 
                                      , stderr = subprocess.STDOUT 
                                      )
        cmd_subproc.wait()
        retcode = cmd_subproc.returncode 
        outfile.close
        in_file = open(outfile_path,'r')
        output = ''.join(in_file.readlines())
        return retcode,output

    """ method creates full backup from server object
        new_backup_object = backup_full(server_object)
    """
    def backup_full(self,server_object):
        self.datadir = server_object.datadir
        self.ib_bin = self.ib_bin_path
        self.xb_bin = self.xb_bin_path
        self.b_root_dir = self.backup_dir
        allocated_dir = self.alloc_dir(self.b_root_dir)
        self.b_path = os.path.join(self.b_root_dir, allocated_dir)
        temp_log = os.path.join(self.b_root_dir, '%s.log' %allocated_dir)
        self.xb_log = os.path.join(self.b_path, '%s.log' %allocated_dir)
        cmd = [self.ib_bin
              , "--defaults-file=%s" %server_object.cnf_file
              , "--no-timestamp"
              , "--user=root"
              , "--port=%d" %server_object.master_port
              , "--host=127.0.0.1"
              , "--ibbackup=%s" %self.xb_bin
              , self.b_path
              ]
        cmd = " ".join(cmd)
        self.retcode, self.output = self.execute_cmd(cmd, self.b_root_dir, temp_log)
        shutil.move(temp_log, self.xb_log)
        self.status = 'full-backup'
        return self

    """ Create incremental backup from full backup and server objects
        Usage: incremental_backup_object = backup_inc(server_obj,previous_backup_object)
    """
    def backup_inc(self,server_object,backup_object):
        self.ib_bin = self.ib_bin_path
        self.xb_bin = self.xb_bin_path
        self.b_root_dir = self.backup_dir
        allocated_dir = self.alloc_dir(self.b_root_dir)
        self.b_path = os.path.join(self.b_root_dir, allocated_dir)
        temp_log = os.path.join(self.b_root_dir, '%s.log' %allocated_dir)
        self.xb_log = os.path.join(self.b_path, '%s.log' %allocated_dir)
        cmd = [self.ib_bin
              , '--defualts-file=%s' %server_object.cnf_file
              , '--no-timestamp'
              , '--user=root'
              , '--port=%d' %server_object.master_port
              , '--host=127.0.0.1'
              , '--ibbackup=%s' %self.xb_bin
              , '--incremental'
              , '--incremental-basedir=%s' %backup_object.b_path
              , self.b_path
              ]
        cmd = " ".join(cmd)
        self.retcode, self.output = self.execute_cmd(cmd, self.b_root_dir, temp_log)
        shutil.move(temp_log, self.xb_log)
        self.status = 'inc-backup'
        return self

    """ Method to prepare a backup
        Usage prepare(unprepared_backup_object)
    """
    def prepare(self,backup_object,rollback=True):
        if rollback:
            rollback = ''
        else:
            rollback = '--redo-only'

        cmd = [backup_object.ib_bin
              , "--apply-log"
              , " %s" %rollback
              , "--ibbackup=%s" %backup_object.xb_bin
              , backup_object.b_path
              ]
        cmd = " ".join(cmd)
        temp_log = os.path.join(backup_object.b_path, self.alloc_dir(backup_object.b_path,dir_pattern='prepare'))
        retcode, output = self.execute_cmd(cmd, backup_object.b_path, temp_log)
        if rollback and retcode==0:
            backup_object.status = 'prepared-redo-only'
        elif not rollback and retcode==0:
            backup_object.status = 'prepared'
        else:
            backup_object.status = 'prepare-failed'

        return retcode, output

    """ Method restores server_object's data from backup
        If backup is not ready for restore an appropriate error
        message will be returned.
        Usage: restore(prepared_backup_object,server_object)
    """
    def restore(self,backup_object,server_object):
        if backup_object.status=='prepared-redo-only' or backup_object.status=='prepared':
            pass
        elif backup_object.status=='full-backup':
            retcode = 1
            output = 'Backup has to be prepared before restore!\n'
            return retcode, output
        elif backup_object.status=='inc-backup':
            retcode = 1
            output = 'You have to apply incrementals to full backup to restore it!\n'
        elif backup_object.status=='prepare-failed':
            retcode = 1
            output = 'Backup failed to prepare, see prepare log for details.\n'
        else:
            retcode = 1
            output = 'CRITICAL ERROR: Unknown backup status!\n'

        cmd = [backup_object.ib_bin
              , "--defaults-file=%s" %server_object.cnf_file
              , "--copy-back"
              , "--ibbackup=%s" %backup_object.xb_bin
              , backup_object.b_path
              ]
        cmd = " ".join(cmd)
        server_object.stop()
        self.clean_dir(server_object.datadir)
        temp_log = os.path.join(backup_object.b_path, self.alloc_dir(backup_object.b_path,dir_pattern='restore'))
        retcode, output = self.execute_cmd(cmd, backup_object.b_path, temp_log)
        server_object.start()
        return retcode, output

