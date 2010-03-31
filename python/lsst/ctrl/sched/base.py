"""
Common classes and functions used across the blackboard submodules.
"""


class _AbstractBase(object):
    """
    a base class that provides some mechanism to support a notion of
    "abstract" classes in python.  It provides a mechanism to discourage
    naive direct user instantiation of the abstract class and a mechanism
    to encourage the implementation of abstract methods in subclasses.
    """
    
    def __init__(self, fromSubclass=False):
        """
        create the base class.  This will do a check, based on the value of
        the fromSubclass parameter, to determine if this constructor appears
        that it is being called directly by the user.  If it is a RuntimeError
        exception is raised.

        @param fromSubclass  a flag that, if true, indicates that it is being
                               called from a subclass's __init__().  By
                               default, this parameter is set to False which
                               will trigger a RuntimeError.  Subclasses that
                               call this constructor should set this to True.
        @throws RuntimeError if fromSubclass is False
        """

        # confirm with caller this we are not instantiating this "abstract"
        # class directly
        _checkAbstract(fromSubclass, "BlackboardItemQueue")

    def _checkAbstract(self, fromSubclass, clsname):
        """
        check the value of the fromSubclass parameter as an indicator of 
        whether access to this class is consistent with its status as
        abstract.  This method is intended to be called in the constructor
        of a subclass that is to be considered abstract.  
        @param fromSubclass   if False, a RuntimeError is raised.
        @param clsname        a name of the class that that where this function
                                 is called.  This is incorporated in the
                                 exception message to indentify the abstract
                                 class being misused.
        """
        if not fromSubclass:
            raise RuntimeError('Programmer error: Apparent instantiation of "abstract" %s class' % clsname)

    def _notImplemented(self, methname):
        """
        raise a RuntimeError indicating that a non-implemented method was
        called.
        @param methname   the name of the method that is not implemented.  This
                            is incorporated into the exception message.
        """
        raise RuntimeError("Programmer error: unimplemented method, %s, for class %s" % (methname, str(self.__class__)))


