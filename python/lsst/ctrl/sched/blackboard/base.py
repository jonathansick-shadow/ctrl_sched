"""
Common classes and functions used across the blackboard submodules.
"""


class _AbstractBase(object):
    
    def __init__(self, fromSubclass=False):
        """create the base"""

        # confirm with caller this we are not instantiating this "abstract"
        # class directly
        _checkAbstract(fromSubclass, "BlackboardItemQueue")

    def _checkAbstract(self, fromSubclass, clsname):
        if not fromSubclass:
            raise RuntimeError('Programmer error: Apparent instantiation of "abstract" %s class' % clsname)

    def _notImplemented(self, methname):
        """
        raise a RuntimeError indicating that a non-implemented method was
        called.
        """
        raise RuntimeError("Programmer error: unimplemented method, %s, for class %s" % (methname, str(self.__class__)))


