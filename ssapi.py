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
        msg = "Error running cmd '{0}: {1}'".format(cmdstr,errdata)
        print( msg )
        raise UserWarning( msg )
    return( outdata )


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

class Filesystem:
    """
    This class will collect the information about the specified GPFS device.
    """
    def __init__( self, gpfsdev ):
        if not gpfsdev:
           raise ValueError('NoDevice')

        self.gpfsdev = gpfsdev
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
            self.filesys[key] = value

    def __getitem__( self, key ):
        return self.filesys[key]
                     

if __name__ == '__main__':
   pass

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
  

