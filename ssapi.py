#!/usr/bin/env python
#=====================================================================================
# Chad Kerner, Senior Systems Engineer
# Storage Enabling Technologies
# National Center for Supercomputing Applications
# ckerner@illinois.edu     chad.kerner@gmail.com
#=====================================================================================
#
# This was born out of a need to programmatically interface with IBM Spectrum
# Scale or the software formerly knows as GPFS.
#
# This was written in Python 2.7 with Spectrum Scale 4.2.  That is what we currently
# have and so that is what I will keep it updated with until other needs arise.
#
# There is NO support, use it at your own risk.  Although I have not coded anything
# too awfully dramatic in here.
#
# If you find a bug, fix it.  Then send me the diff and I will merge it into the code.
#
# You may want to pull often because this is being updated quite frequently as our
# needs arise in our clusters.
#
#=====================================================================================


from __future__ import print_function
from subprocess import Popen, PIPE
import sys
import shlex
import time
import socket


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


def replace_encoded_strings( mystring ):
    """
    The mmlsfileset command returns encoded strings for special characters.
    This will replace those encoded strings and return a true string.
    """
    tempstring = mystring.replace('%2F', '/')
    mystring = tempstring
    tempstring = mystring.replace('%3A', ':')
    return tempstring


def remove_special_characters( mystring ):
    """
    The mmlspool command has returns strings with special characters.
    This will remove those encoded strings.
    """
    tempstring = mystring.replace('%', '')
    mystring = tempstring
    tempstring = mystring.replace('(', '')
    mystring = tempstring
    tempstring = mystring.replace(')', '')
    return tempstring


class Nsds:
    """
    This class contains all of the information about the NSDs in the cluster.  It
    includes the NSD name, servers they are attached to, gpfs device they serve, etc.

    gpfsdevs - a list unique gpfs devices

    nsds[name]['usage'] = The gpfs device the specified name is a part of
    nsds[name]['servers'] = The storage servers the specified name is hosted by

    """
    def __init__( self ):
        self.collect_nsd_info()

    def dump( self ):
        print("GPFS Devices: {}".format(self.gpfsdevs))
        nsd_keys = self.nsds.keys()
        for nsd in sorted(nsd_keys):
            print("{:<10s}  {:<10s}  {:<s}".format(nsd, self.nsds[nsd]['usage'], self.nsds[nsd]['servers']))

    def return_gpfs_devices( self ):
        """
        Return an iterable list of uniq GPFS devices.
        """
        return self.gpfsdevs

    def collect_nsd_info( self ):
        """
        Process the mmlsnsd command output to build the necessary structures.
        """
        self.nsds = {}
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

            if '(local cache)' in line:
               nsd_name = line.split()[2]
               fsname = 'lroc'
               servers = (line.split()[3]).split(',')
            elif 'free disk' in line:
               nsd_name = line.split()[2]
               fsname = 'free'
               servers = (line.split()[3]).split(',')
            else:
               nsd_name = line.split()[1]
               fsname = line.split()[0]
               servers = (line.split()[2]).split(',')
               fsdevs[fsname] = 1

            self.nsds[nsd_name] = {}
            self.nsds[nsd_name]['usage'] = fsname
            self.nsds[nsd_name]['servers'] = servers

            try:
               del fsname
               del servers
            except NameError:
               pass

        self.gpfsdevs = fsdevs.keys()



class Cluster:
    """
    This class will collect the information about the cluster.

    If you wish to debug the class, you can now pass Debug=True on the initialization.
    """
    def __init__( self, Debug=False ):
        self.set_debug( Debug )
        self.get_cluster_info()
        self.get_node_name()
        self.is_node_cluster_manager()
        self.nsds = Nsds()
        self.gpfsdevs = self.nsds.return_gpfs_devices()

    def set_debug( self, Debug ):
        """
        Set the debugging level for the class. 0 by default.
        """
        self.debug = Debug
        if self.debug==True:
           print("DEBUG: Debugging turned on")

    def toggle_debug( self ):
        """
        Toggle debugging. If on, shut it off, if off, set it to 1.
        """
        if self.debug == True:
           print("DEBUG: Turning ssapi debugging off.")
           self.debug = False
        else:
           print("DEBUG: Turning ssapi debugging on.")
           self.debug = True

    def get_node_name( self ):
        """
        This routine will extract the current node name from the GPFS configuration file.
        """
        f = open('/var/mmfs/gen/mmfsNodeData', 'r')
        nodecfg = f.read()
        nodecfg_s = nodecfg.split(':')
        f.close()
        self.nodename = nodecfg_s[5]

    def get_cluster_manager( self ):
        """
        This routine parses the mmlsmgr command.
        """
        self.cluster_manager = {}
        cmd_out = run_cmd("/usr/lpp/mmfs/bin/mmlsmgr -c")
        for line in cmd_out.splitlines():
            line.rstrip()

            # Ignore blank lines
            if not line:
               continue

            if 'Cluster manager node' in line:
               line = line.translate(None, '()')
               ipaddr = line.split()[3]
               nodename = line.split()[4]
               self.cluster_manager['node'] = nodename
               self.cluster_manager['ip'] = ipaddr
               if self.debug:
                  print("DEBUG: Cluster Manager IP: {0}".format(ipaddr))
                  print("DEBUG: Cluster Manager Name: {0}".format(nodename))

    def is_node_cluster_manager( self ):
        """
        Check to see if this node is the cluster manager.  If so, 
        set cluster_manager to True else set it to False.
        """
        self.get_cluster_manager()

        if self.nodename in self.cluster_manager['node']:
           self.is_cluster_manager = True
        else:
           self.is_cluster_manager = False

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
        if self.debug:
           print("Cluster Information")
           for key in self.cluster_info.keys():
               print("{0} -> {1}".format(key, self.cluster_info[key]))

           print("\nNSD Information")
           for key in self.nsd_info.keys():
               print("{0} -> FS: {1}   Servers: {2}".format(key, self.nsd_info[key]['usage'], self.nsd_info[key]['servers']))





class StoragePool:
    def __init__( self, gpfsdev ):
        self.gpfsdev = gpfsdev
        self.pools = {}

        cmd_out = run_cmd("/usr/lpp/mmfs/bin/mmlspool {}".format( self.gpfsdev))

        for line in cmd_out.splitlines()[2:]:
            line.rstrip()

            # Ignore blank lines
            if not line:
               continue

            newline = remove_special_characters( line )
            vals = newline.split()
            poolname = vals[0]
            self.pools[poolname] = {}
            self.pools[poolname]['id'] = vals[1]
            self.pools[poolname]['blksize'] = vals[2]
            self.pools[poolname]['blkmod'] = vals[3]
            self.pools[poolname]['data'] = vals[4]
            self.pools[poolname]['metadata'] = vals[5]
            self.pools[poolname]['datasize'] = vals[6]
            self.pools[poolname]['datafree'] = vals[7]
            self.pools[poolname]['datapctfree'] = vals[8]
            self.pools[poolname]['metasize'] = vals[9]
            self.pools[poolname]['metafree'] = vals[10]
            self.pools[poolname]['metapctfree'] = vals[11]

        self.pool_list = self.pools.keys()


    def dump( self ):
        print("{}".format(self.pools))


    def __getitem__( self, key ):
        return self.pools[key]


class Snapshots:
    def __init__( self, gpfsdev, fileset='' ):
        self.gpfsdev = gpfsdev
        self.fileset = fileset
        self.snap_name_separator = '_'
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
                self.snapshots[sname][keys[idx]] = replace_encoded_strings( vals[idx] )

        snaplist = self.snapshots.keys()
        self.snaplist = sorted( snaplist )
        self.snap_count = len( snaplist )



    def get_delete_list( self, max_to_keep ):
        """
        Given an integer of how many snapshots you want to keep, this will return a list of snapshot
        names that are to be purged.  It does not purge them, only a list of what needs to be purged
        based on how many you want to keep.
        """
        self.dellist = []
        if self.snap_count <= max_to_keep:
           return self.dellist

        if max_to_keep == 0:
           self.dellist = self.snaplist
        elif self.snap_count > max_to_keep:
           self.dellist = list(self.snaplist)[ : -( max_to_keep ) ]

        return self.dellist


    def delsnap( self, snap_name ):
        """
        Given a specific snapshot name, this routine will execute mmdelsnapshot and return you the output
        from the command.  The object already knows if it is a filesystem or a fileset snapshot, so you just
        need to specify the snapshot name.
        """
        if self.fileset == '':
           cmd_out = run_cmd("/usr/lpp/mmfs/bin/mmdelsnapshot {} {}".format(self.gpfsdev, snap_name))
        else:
           cmd_out = run_cmd("/usr/lpp/mmfs/bin/mmdelsnapshot {} {} -j {}".format(self.gpfsdev, snap_name, self.fileset))
        return cmd_out


    def snap( self ):
        """
        This code will create a snapshot of the specified filesystem or fileset.

        NOTE: You can NOT mix filesystem and fileset snapshots on the same GPFS device.

        Filesystem snapshots are named: CCYYMMDD==HHMM for easy sorting / processing.

        Filesystem snapshots are named: <Fileset>==CCYYMMDD==HHMM for easy processing again.
        """
        if self.fileset == '':
           snapname = time.strftime("%Y%m%d") + self.snap_name_separator + time.strftime("%H%M")
           cmd_out = run_cmd("/usr/lpp/mmfs/bin/mmcrsnapshot {0} {1}".format( self.gpfsdev, snapname ))
        else:
           snapname = self.fileset + self.snap_name_separator + time.strftime("%Y%m%d") + self.snap_name_separator + time.strftime("%H%M")
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
           self.get_pool_information()


    def print_keys( self ):
        keys = self.filesys.keys()
        return keys


    def get_pool_information( self ):
        self.pools = StoragePool( self.gpfsdev )

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
            self.filesys[key] = replace_encoded_strings( value )


    def fileset_list( self ):
        """
        Return all of the fileset names in the file system.
        """
        return self.filesets.keys()


    def independent_inode_fileset( self, fname ):
        if self.filesets[fname]['fstype'] == 'Independent':
           return True
        else:
           return False


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
                self.filesets[fname][keys[idx]] = replace_encoded_strings( vals[idx] )

            # Set the fileset type. independent inode or dependent inode
            if self.filesets[fname]['filesetName'] == 'root' and self.filesets[fname]['inodeSpace'] == '0':
               self.filesets[fname]['fstype'] = 'Independent'
            elif self.filesets[fname]['inodeSpace'] >= '1':
               self.filesets[fname]['fstype'] = 'Independent'
            elif self.filesets[fname]['inodeSpace'] == '0':
               self.filesets[fname]['fstype'] = 'Dependent'
            else:
               self.filesets[fname]['fstype'] = 'Unknown'




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

   sys.exit(0)

   snap = Snapshots( 'condo', 'root' )

   #myFS = Filesystem( 'condo' )
   #fslist = myFS.fileset_list()
   #print("{}".format(myFS.filesets))


   #newfs = Filesystem.Create( 'fs0', 'chad' )

   #Clstr = Cluster()
   #Clstr.dump()

   #print(Clstr.gpfsdevs)
   #myFs = Filesystem( 'wvu' )
   #FSa = Filesystem( 'des003' )

   #print(myFs['disks'])
   #print(FSa['disks'])

   #try:
   #  F = Filesystem('')
   #except:
   #  print("No Filesystem device specified.")
   #else:
   #  print(F[disks])


