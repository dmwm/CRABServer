#!/usr/bin/env python
class ChangeFileState:
    sql = """UPDATE filemetadata SET fmd_filestate=:filestate WHERE fmd_lfn=:outlfn and tm_taskname=:taskname
          """
