# ssapi
Spectrum Scale Python API

This module interfaces with IBM Spectrum Scale commands to query/modify
things in the cluster.

```
 (! 1002)-> python
 Python 2.7.3 (default, Aug  7 2013, 23:04:52)
 [GCC 4.4.7 20120313 (Red Hat 4.4.7-3)] on linux2
 Type "help", "copyright", "credits" or "license" for more information.
 >>> from ssapi import *
 >>> f = Filesystem('chad')
 >>> print f['disks']
 meta_03_01;meta_03_02;meta_03_03;meta_03_04
 >>> print f['storagePools']
 system;6000;6001;6002
```
