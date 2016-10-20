#!/usr/bin/env python
#
# This is a work in progress, adding functionality as I need it.
#
# Chad Kerner - chad.kerner@gmail.com
#


from __future__ import print_function
from subprocess import Popen, PIPE
import sys
import shlex
import time

def run_cmd( cmdstr=None ):
    """
    Wrapper around subprocess module calls.
    """
    if not cmdstr:
        return None
    cmd = shlex.split(cmdstr)
    subp = Popen(cmd, stdout=PIPE, stderr=PIPE)
    (outdata, errdata) = subp.communicate()
    if subp.returncode != 0:
        msg = "Error\n  Command: {0}\n  Message: {1}".format(cmdstr,errdata)
        raise UserWarning( msg )
        sys.exit( subp.returncode )
    return( outdata )


def replace_string( mystring ):
    """
    The mmlsfileset command returns encoded strings for special characters. 
    This will replace those encoded strings and return a true string.
    """
    tempstring = mystring.replace('%2F', '/')
    mystring = tempstring
    tempstring = mystring.replace('%3A', ':')
    return tempstring


class Cluster:
    """
    This class will collect the information about the cluster.
    """
    def __init__( self ):
        self.get_cluster_info()
        self.get_nsd_info()

    def get_nsd_info( self ):
        """
        This routing parses the mmlsnsd command.
        """
        self.nsd_info = {}
        fsdevs = {}
        cmd_out = run_cmd("/usr/lpp/mmfs/bin/mmlsnsd")

        for line in cmd_out.splitlines():
            line.rstrip()

            # Ignore blank lines
            if not line:
               continue

            # Ignore dashed lines
            if '----------' in line:
               continue

            # Ignore header lines
            if 'File system' in line:
               continue

            if 'free disk' in line:
               nsd_name = line.split()[2]
               fsname = 'free'
               servers = (line.split()[3]).split(',')
            else:
               nsd_name = line.split()[1]
               fsname = line.split()[0]
               servers = (line.split()[2]).split(',')
               fsdevs[fsname] = 1

            self.nsd_info[nsd_name] = {} 
            self.nsd_info[nsd_name]['usage'] = fsname
            self.nsd_info[nsd_name]['servers'] = servers
            
        self.gpfsdevs = fsdevs.keys()
        
 
    def get_cluster_info( self ):
        """
        This routine parses the mmlscluster command.
        """
        self.cluster_info = {}
        cmd_out = run_cmd("/usr/lpp/mmfs/bin/mmlscluster")
        found_nodes = 0
        for line in cmd_out.splitlines():
            line.rstrip()

            # Ignore blank lines
            if not line:
               continue

            # Ignore dashed lines
            if '----------' in line:
               continue
            if '==========' in line:
               continue

            if found_nodes >= 1:
               nodeid = line.split()[0]
               daemonname = line.split()[1]
               ipaddr = line.split()[2]
               adminname = line.split()[3]
               self.cluster_info['nodes'][nodeid] = {}
               self.cluster_info['nodes'][nodeid]['daemon_name'] = daemonname
               self.cluster_info['nodes'][nodeid]['ip'] = ipaddr
               self.cluster_info['nodes'][nodeid]['admin_name'] = adminname
 
            if 'Node  Daemon' in line:
               found_nodes = found_nodes + 1
               self.cluster_info['nodes'] = {}
            
            if 'GPFS cluster name' in line:
               self.cluster_info['name'] = line.split()[3]
            if 'GPFS cluster id' in line:
               self.cluster_info['id'] = line.split()[3]
            if 'GPFS UID domain' in line:
               self.cluster_info['uid'] = line.split()[3]
            if 'Remote shell command' in line:
               self.cluster_info['rsh'] = line.split()[3]
            if 'Remote file copy command' in line:
               self.cluster_info['rcp'] = line.split()[4]
            if 'Primary server' in line:
               self.cluster_info['primary'] = line.split()[2]
            if 'Secondary server' in line:
               self.cluster_info['secondary'] = line.split()[2]
            
    
    def dump( self ):
        if debug == 1:
           print("Cluster Information")
           for key in self.cluster_info.keys():
               print("{0} -> {1}".format(key, self.cluster_info[key]))

        if debug == 2:
           print("\nNSD Information")
           for key in self.nsd_info.keys():
               print("{0} -> FS: {1}   Servers: {2}".format(key, self.nsd_info[key]['usage'], self.nsd_info[key]['servers']))

class Snapshots:
    def __init__( self, gpfsdev, fileset='' ):
        self.gpfsdev = gpfsdev
        self.fileset = fileset
        self.snapshots = {}

        if self.fileset == '':
           cmd_out = run_cmd("/usr/lpp/mmfs/bin/mmlssnapshot {0} -Y".format( self.gpfsdev ))
        else:
           cmd_out = run_cmd("/usr/lpp/mmfs/bin/mmlssnapshot {0} -j {1} -Y".format( self.gpfsdev, self.fileset ))

        # Process the HEADER line
        if 'No snapshots in file system' in cmd_out.splitlines()[0]:
           self.snap_count = 0
           return

        keys = cmd_out.splitlines()[0].split(':')[6:]

        for line in cmd_out.splitlines()[1:]:
            line.rstrip()

            # Ignore blank lines
            if not line:
               continue

            vals = line.split(':')[6:]
            sname = vals[1]
            self.snapshots[sname] = {}
            for idx in range(len(vals)-1):
                self.snapshots[sname][keys[idx]] = replace_string( vals[idx] )

        snaplist = self.snapshots.keys()
        self.snaplist = sorted( snaplist )
        self.snap_count = len( snaplist )


    def snap( self ):
        if self.fileset == '':
           snapname = 'FILESYSTEM==' + time.strformat("%Y%m%d") + '==' + time.strftime("%H%M")
           cmd_out = run_cmd("/usr/lpp/mmfs/bin/mmcrsnapshot {0} {1}".format( self.gpfsdev, snapname ))
        else:
           snapname = self.fileset + '==' + time.strformat("%Y%m%d") + '==' + time.strftime("%H%M")
           cmd_out = run_cmd("/usr/lpp/mmfs/bin/mmcrsnapshot {0} {1} -j {2}".format( self.gpfsdev, snapname, self.fileset ))


class Filesystem:
    """
    This class will collect the information about the specified GPFS device.
    """

    filesystem_defaults = { 'automaticMountOption': 'yes', 
                            'defaultMetadataReplicas': '1', 
                            'maxMetadataReplicas': '2', 
                            'defaultDataReplicas': '1', 
                            'maxDataReplicas': '2', 
                            'blockAllocationType': '',
                            'fileLockingSemantics': '',
                          }

    def __init__( self, gpfsdev ):
        if not gpfsdev:
           raise ValueError('NoDevice')
        else:
           self.gpfsdev = gpfsdev
           self.get_filesystem_information()
           self.get_fileset_information()


    def print_keys( self ):
        keys = self.filesys.keys()
        return keys


    def get_filesystem_information( self ):
        self.filesys = {}
        cmd_out = run_cmd("/usr/lpp/mmfs/bin/mmlsfs {0} -Y".format(self.gpfsdev))
        for line in cmd_out.splitlines():
            line.rstrip()

            # Ignore blank lines
            if not line:
               continue

            # Ignore HEADER line
            if 'HEADER' in line:
               continue

            key = line.split(':')[7]  
            value = line.split(':')[8]  
            self.filesys[key] = replace_string( value )


    def fileset_list( self ):
        """
        Return all of the fileset names in the file system.
        """
        return self.filesets.keys()


    def get_fileset_information( self ):
        self.filesets = {}
        cmd_out = run_cmd("/usr/lpp/mmfs/bin/mmlsfileset {0} -Y".format(self.gpfsdev))

        # Process the HEADER line
        keys = cmd_out.splitlines()[0].split(':')[7:]

        for line in cmd_out.splitlines()[1:]:
            line.rstrip()

            # Ignore blank lines
            if not line:
               continue

            vals = line.split(':')[7:]
            fname = vals[0]
            self.filesets[fname] = {}
            for idx in range(len(vals)-1):
                self.filesets[fname][keys[idx]] = replace_string( vals[idx] )


    @classmethod
    def Create( self, gpfsdev, fsname ):
        """ 
        This function will create a new filesystem.

        Input 1: A dictionary containing the nsd's and their parameters.
           mydisks['nsd1']['usage']='dataAndMetadata'
           mydisks['nsd1']['failuregroup']='-1'
           mydisks['nsd1']['pool']='system'

        Input 2: A dictionary containing parameters for the file system.
        """
        print("Creating {0} on {1}".format(fsname, gpfsdev))

        self = Filesystem( gpfsdev )
        return self


    def __getitem__( self, key ):
        return self.filesys[key]
                     

if __name__ == '__main__':
   #
   # This is just where I do my testing of stuff.
   #

   snap = Snapshots( 'condo', 'root' )

   sys.exit(0)

   myFS = Filesystem( 'condo' )
   fslist = myFS.fileset_list()
   print("{}".format(myFS.filesets))


   newfs = Filesystem.Create( 'fs0', 'chad' )

   Clstr = Cluster()
   #Clstr.dump()

   print(Clstr.gpfsdevs)
   myFs = Filesystem( 'wvu' )
   FSa = Filesystem( 'des003' )

   print(myFs['disks'])
   print(FSa['disks'])

   try:
     F = Filesystem('')
   except:
     print("No Filesystem device specified.")
   else:
     print(F[disks])
  

