"""
    Attrib - a quick reimplementation of the nose test attrib stuff
             here so that nose doesn't have to be installed to get
             things to work
"""

def attr(*args, **kwargs):
    """Decorator that adds attributes to classes or functions
    for use with the Attribute (-a) plugin.
    """
    def wrap_ob(ob):
        for name in args:
            setattr(ob, name, True)
        for name, value in kwargs.iteritems():
            setattr(ob, name, value)
        return ob
    return wrap_ob
