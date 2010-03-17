"""
Exceptions for interacting with blackboards
"""
import re

class BlackboardAccessError(Exception):
    """
    a generic error accessing the blackboard state.

    This is the parent exception of all blackboard exceptions.
    """
    def __init__(self, msg=None, wrapped=None):
        """
        create the exception
        @param msg      the message describing the problem.  If not provided,
                            a default will be created.
        @param wrapped  an optional wrapped exception representing the
                            underlying reason for the failure.
        """
        if not msg:
            name= None
            if wrapped:
                msg = "Access error due to %s: %s" % \
                      (self._excname(wrapped), str(wrapped))
            else:
                msg = "Unknown error during blackboard access"
                
        Exception.__init__(self, msg)

        # the wrapped exception
        self.wrapped = None

    def _excname(self, excp):
        name = repr(excp.__class__)
        typematch = re.match(r"<type '([^']+)'>", name)
        if typematch:
            name = typematch.group(1)

        if name.startswith("exceptions."):
            name = name[11:]
        return name
            

class BlackboardUpdateError(BlackboardAccessError):
    """
    a failure was encountered while trying to update the state of a
    Blackboard queue.
    """
    def __init__(self, msg=None, wrapped=None):
        if not msg:
            name= None
            if wrapped:
                msg = "Update error due to %s: %s" % \
                      (self._excname(wrapped), str(wrapped))
            else:
                msg = "Unknown error during blackboard update"
                
        Exception.__init__(self, msg, wrapped)

class BlackboardPersistError(BlackboardUpdateError):
    """
    a failure was encountered while trying to persist the state of a
    Blackboard queue to disk.
    """
    def __init__(self, msg=None, wrapped=None):
        if not msg:
            msg = "IO failure while updating Blackboard"
        Exception.__init__(self, msg, wrapped)

class BlackboardRollbackError(BlackboardUpdateError):
    """
    a failure occurred while trying to role back changes after detecting
    another failure updating a Blackboard.  The Blackboard may well be
    corrupted.
    """
    def __init__(self, wrapped=None, rollback=None, msg=None):
        if not msg:
            if rollback:
                msg = "Rollback failure: %s" % str(rollback)
            else:
                msg = "Unknown rollback failure"
        Exception.__init__(self, msg, wrapped)
        self._rollback = rollback
