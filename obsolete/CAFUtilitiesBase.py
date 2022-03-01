"""
	CAFUtilitiesBase.py
		Helper function to find the base of the CAFUtilities install
"""
import os.path

def getCAFUtilitiesBase():
    """ returns the root of CAFUtilities install """
    if __file__.find("src/python") != -1:
        return os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
    else:
        return os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))
