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
        @param isstaticset  this filter implies a closed, static set of 
                            identifiers that will be recognized.  If True,
                            allowedValues() is guaranteed to return without 
                            error; otherwise, its answer should not be trusted
        """
        self._checkAbstract(fromSubclass, "IDFilter")
        self.name = name
        self.outname = outname
        if self.outname is None:
            self.outname = name
        self.isstatic = False

    def hasStaticValueSet(self):
        """
        return True if the set of values returned by allowedValues() is 
        considered the complete set of allowed identifiers returned by 
        recognize().  If False is returned, allowedValues() may raise 
        a RuntimeError.
        """
        return self.isstatic

    def allowedValues(self):
        """
        return a list representing the complete set of values that will
        be returned by recognize() (except None).  This may raise an 
        exception if hasStaticValueSet() returns False.

        This implementation always raises a RuntimeError.
        """
        self._notImplemented("allowedValues")

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

    def __init__(self, name, min=None, lim=None, values=None,
                 isstaticset=True):
        """
        create the filter
        @param min     the minimum identifier value recognized
        @param lim     one more than the maximum identifier value recognized
        @param values  an arbitrary list of identifier values recognized.  
                         These may be listed in addition to or instead of 
                         a range.
        @param isstaticset  a flag indicating whether these parameters should
                         be considered a closed, static set of identifiers.
                         While by default this is set to True, it will be 
                         internally made False if one and only of min and lim
                         are specified.  
        """
        IDFilter.__init__(self, name, fromSubclass=True)
        self.isstatic = isstaticset
        self.range = (min, lim)
        self.values = None
        if values is not None:
            if not isinstance(values, list):
                values = [values]
            if len(filter(lambda v: not isinstance(v,int), values)) > 0:
                raise ValueError("IntegerIDFilter: non-integer value given for values: " + str(self.values))
            self.values = list(values)

        if any(self.range) and any(filter(lambda r: r is None, self.range)):
            self.isstatic = False

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

    def allowedValues(self):
        """
        return a list representing the complete set of values that will
        be returned by recognize() (except None).  This may raise an 
        exception if hasStaticValueSet() returns False.
        """
        if any(self.range) and any(filter(lambda r: r is None, self.range)):
            raise RuntimeError("identifier set (%s) is not closed" % self.name)

        out = range(self.range[0], self.range[1])
        if self.values:
            out.extend(self.values)
            out.sort()

        return out


    @staticmethod
    def fromPolicy(policy):
        """
        create an IntegerIDFilter from an "id" policy
        """
        name = "unknown"
        min = lim = vals = None
        if policy.exists("name"):    name = policy.getString("name")
        if policy.exists("min"):     min  = policy.getInt("min")
        if policy.exists("lim"):     lim  = policy.getInt("lim")
        if policy.exists("values"):  vals = policy.getArray("values")

        return IntegerIDFilter(name, min, lim, vals)

IDFilter.classLookup["Integer"] = IntegerIDFilter
IDFilter.classLookup["IntegerIDFilter"] = IntegerIDFilter
