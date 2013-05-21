"""
	CRABServerBase.py
		Helper function to find the base of the CRABServer install
"""
import os.path

def getCRABServerBase():
    """ returns the root of CRABServer install """
    if __file__.find("src/python") != -1:
        return os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
    else:
        return os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))
