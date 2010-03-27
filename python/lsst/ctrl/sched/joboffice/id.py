"""
classes for filtering identifiers based on constraints
"""
from __future__ import with_statement

from lsst.pex.policy import Policy
from lsst.ctrl.sched.blackboard.base import _AbstractBase

class IDFilter(_AbstractBase):
    """
    a class for recognizing an identfier matching a set of policy-encoded 
    constraints
    """

    def __init__(self, name, outname=None, fromSubclass=False):
        """
        create the filter
        @param name      a name for identifier to be presented to the filter.
        @param outname   a name for the identifier that will be returned.  If 
                            None, the input name is set.
        """
        self._checkAbstract(fromSubclass, "IDFilter")
        self.name = name
        self.outname = outname
        if self.outname is None:
            self.outname = name

    def recognize(self, id):
        """
        return an identifier value associated with the given input 
        identifier or None if the input is not recognized.

        The implimentation need not return the same identifier 
        as passed in.  
        @param id   the input identifier to recognize.  The type and 
                        contents are assumptions of the implementation
        """
        self._notImplemented("recognize")

    classLookup = { }

    @staticmethod
    def fromPolicy(policy):
        clsname = "IntegerIDFilter"
        if policy.exists("className"):
            clsname = policy.getString("className")

        cls = None
        if IDFilter.classLookup.has_key(clsname):
            cls = IDFilter.classLookup[clsname]

        else:
            # lookup a fully qualified class
            raise RuntimeError("programmer error class name lookup not implemented")

        return cls.fromPolicy(policy)

class IntegerIDFilter(IDFilter):
    """
    a class for recognizing an identfier matching a set of policy-encoded 
    constraints
    """

    def __init__(self, name, min=None, lim=None, values=None):
        """
        create the filter
        """
        IDFilter.__init__(self, name, fromSubclass=True)
        self.range = (min, lim)
        self.values = None
        if values is not None:
            if not isinstance(values, list):
                values = [values]
            if len(filter(lambda v: not isinstance(v,int), values)) > 0:
                raise ValueError("IntegerIDFilter: non-integer value given for values: " + str(self.values))
            self.values = list(values)

    def recognize(self, id):
        """
        return an identifier value associated with the given input 
        identifier or None if the input is not recognized.

        This implimentation returns the input identifier (as a string) 
        if it is recongized.
        """
        if not isinstance(id, int):
            id = int(id)

        if any(self.range):
            if self.range[1] is None and id >= self.range[0]:
                return id
            elif self.range[0] is None and id < self.range[1]:
                return id
            elif id >= self.range[0] and id < self.range[1]:
                return id

        if self.values and id in self.values:
            return id

        return None

    @staticmethod
    def fromPolicy(policy):
        """
        create an IntegerIDFilter from an "id" policy
        """
        name = "unknown"
        min = lim = vals = None
        if policy.exists("name"):    name   = policy.getString("name")
        if policy.exists("min"):     min    = policy.getInt("min")
        if policy.exists("lim"):     max    = policy.getInt("lim")
        if policy.exists("values"):  values = policy.getArray("values")

        return IntegerIDFilter(name, min, lim, vals)

IDFilter.classLookup["Integer"] = IntegerIDFilter
IDFilter.classLookup["IntegerIDFilter"] = IntegerIDFilter
