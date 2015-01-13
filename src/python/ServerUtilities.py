"""
This contains some utility methods to share between the server and the client
"""

def checkOutLFN(lfn, username):
    if lfn.startswith('/store/group/'):
        if len(lfn.split('/')) < 5 or lfn.split('/')[3] == username or username not in lfn:
            return False
    elif lfn.startswith('/store/user/'):
        if lfn.split('/')[3] != username:
            return False
    elif lfn.startswith('/store/local/'):
        if lfn.split('/')[3] == '':
            return False
    else:
        return False
    return True 
