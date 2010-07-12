# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#

"""
classes for filtering identifiers based on constraints
"""
from __future__ import with_statement

from lsst.pex.policy import Policy, DefaultPolicyFile
from lsst.ctrl.sched.base import _AbstractBase

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

    def isUnconstrained(self):
        """
        return true if this identifier is not constrained to a set of
        values.  This implies two things:  any value passed to recognize()
        will be recognized, and allowedValues() returns an empty set.
        """
        self._notImplemented("isUnconstrained")

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
        clsname = "StringIDFilter"
        if policy.exists("className"):
            clsname = policy.getString("className")
        elif policy.isInt("min") or policy.isInt("lim") or policy.isInt("value"):
            clsname = "IntegerIDFilter"

        cls = None
        if IDFilter.classLookup.has_key(clsname):
            cls = IDFilter.classLookup[clsname]

        else:
            # lookup a fully qualified class
            raise RuntimeError("programmer error class name lookup not implemented")

        return cls.fromPolicy(policy)

class StringIDFilter(IDFilter):
    """
    a class for recognizing a string-typed identifier matching a set of
    allowed values.  
    """

    def __init__(self, name, values=None, isstaticset=True):
        """
        create the filter
        @param name    the name of the identifier
        @param values  the list of allowed values.
        @param isstaticset  a flag indicating whether these parameters should
                         be considered a closed, static set of identifiers.
                         While by default this is set to True, it will be 
                         internally made False if one and only of min and lim
                         are specified.  
        """
        IDFilter.__init__(self, name, fromSubclass=True)
        self.isstatic = isstaticset

        self.values = None
        if values is not None:
            if not isinstance(values, list):
                values = [values]
            self.values = filter(lambda v: v is not None, values)
            bad = filter(lambda v: not isinstance(v, str), self.values)
            if len(bad) > 0:
                raise ValueError("non-string value(s): " + str(bad))

        if self.isUnconstrained():
            self.isstatic = False
    
    def isUnconstrained(self):
        return not self.values

    def recognize(self, id):
        """
        return an identifier value associated with the given input 
        identifier or None if the input is not recognized.

        This implimentation returns the input identifier (as a string) 
        if it is recongized.
        """
        id = str(id)
        if not self.values or id in self.values:
            return id

        return None

    def allowedValues(self):
        """
        return a list representing the complete set of values that will
        be returned by recognize() (except None).  This may raise an 
        exception if hasStaticValueSet() returns False.
        """
        if not self.hasStaticValueSet():
            raise RuntimeError("identifier set (%s) is not closed" % self.name)

        out = []
        if self.values:
            out.extend(self.values)
            out.sort()
        return out

    _dictionary = None
        
    @staticmethod
    def fromPolicy(policy):
        """
        create an IntegerIDFilter from an "id" policy
        """
        if not StringIDFilter._dictionary:
            pdf = DefaultPolicyFile("ctrl_sched", "StringIDFilter_dict.paf",
                                    "policies")
            StringIDFilter._dictionary = Policy.createPolicy(pdf)
        p = Policy(policy, True)
        if StringIDFilter._dictionary:
            p.mergeDefaults(StringIDFilter._dictionary)

        name = "unknown"
        vals = None
        if policy.exists("name"):   name = policy.getString("name")
        if policy.exists("value"):  vals = policy.getArray("value")

        return StringIDFilter(name, vals)

IDFilter.classLookup["String"] = StringIDFilter
IDFilter.classLookup["StringIDFilter"] = StringIDFilter

class IntegerIDFilter(IDFilter):
    """
    a class for recognizing an integer-typed identfier falling within a
    prescribed range or matching a set of allowed values.
    """

    def __init__(self, name, min=None, lim=None, values=None,
                 isstaticset=True):
        """
        create the filter
        @param name    the name of the identifier
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

        if len(filter(lambda r: r is not None, self.range)) > 0 and \
           len(filter(lambda r: r is None, self.range)) > 0:
            self.isstatic = False
        if self.isUnconstrained():
            self.isstatic = False

    def isUnconstrained(self):
        return not self.values and not any(self.range)

    def recognize(self, id):
        """
        return an identifier value associated with the given input 
        identifier or None if the input is not recognized.

        This implimentation returns the input identifier (as a string) 
        if it is recongized.
        """
        if not isinstance(id, int):
            try:
                id = int(id)
            except:
                return None
        if self.isUnconstrained():
            return id

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
        nones = len(filter(lambda r: r is None, self.range))
        if nones == 1:
            raise RuntimeError("identifier set (%s) is not closed" % self.name)

        if nones == 0:
            out = range(self.range[0], self.range[1])
        else:
            out = []
        if self.values:
            out.extend(self.values)
            out.sort()

        return out

    _dictionary = None

    @staticmethod
    def fromPolicy(policy):
        """
        create an IntegerIDFilter from an "id" policy
        """
        if not IntegerIDFilter._dictionary:
            pdf = DefaultPolicyFile("ctrl_sched", "IntegerIDFilter_dict.paf",
                                    "policies")
            IntegerIDFilter._dictionary = Policy.createPolicy(pdf)
        p = Policy(policy, True)
        if IntegerIDFilter._dictionary:
            p.mergeDefaults(IntegerIDFilter._dictionary)

        name = "unknown"
        min = lim = vals = None
        if policy.exists("name"):   name = policy.getString("name")
        if policy.exists("min"):    min  = policy.getInt("min")
        if policy.exists("lim"):    lim  = policy.getInt("lim")
        if policy.exists("value"):  vals = policy.getArray("value")

        return IntegerIDFilter(name, min, lim, vals)

IDFilter.classLookup["Integer"] = IntegerIDFilter
IDFilter.classLookup["IntegerIDFilter"] = IntegerIDFilter
