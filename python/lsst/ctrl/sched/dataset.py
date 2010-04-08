"""
classes for describing datasets.
@author Ray Plante
"""
from __future__ import with_statement

from lsst.pex.policy import Policy

import os

class Dataset(object):
    """
    a description of a dataset.  

    This description is characterized by a dataset type name and a 
    set of identifiers.  These attributes are access via public member 
    variables 'type' (a string) and ids (a dictionary), respectively.
    """

    def __init__(self, type, path=None, valid=True, ids=None, **kw):
        """
        create the dataset
        @param type    the dataset type name
        @param path    a filesystem pathname to the file.  If None, the 
                         path is not known/applicable
        @param valid   a boolean flag indicating whether this refers to
                         valid dataset.  This is set to False, for example,
                         if the dataset was not successfully created.
        @param ids     a dictionary of identifiers, mapping names to values.
                         the type of the identifier is context specific.
        @param *       additional named parameters are taken as 
                         identifiers to be set with the given values
        """
        self.type = type
        self.path = path
        self.valid = valid

        self.ids = None
        if ids:
            self.ids = dict(ids)
        if kw:
            if self.ids is None:
                self.ids = {}
            for key in kw.keys():
                self.ids[key] = kw[key]

    def __eq__(self, other):
        """
        return True if the given Dataset describes the same data as this
        one.  
        """
        if not isinstance(other, Dataset):
            return False
        if other.type != self.type:
            return False
        if len(filter(lambda d: d.ids is None, [self, other])) == 1:
            return False
        
        keys = other.ids.keys()
        if len(keys) != len(self.ids):
            return False
        for key in keys:
            if not self.ids.has_key(key) or other.ids[key] != self.ids[key]:
                return False
        return True

    def toString(self, usePath=True):
        """
        return a string form if this dataset's contents
        @param usePath   if true, the path will be used available
        """
        if usePath and self.path:
            return self.path
        out = self.type
        if self.ids is not None:
            for id in self.ids:
                out += "-%s%s" % (id, self.ids[id])
        return out

    def __str__(self):
        return self.toString()

    def toPolicy(self, policy=None):
        """
        return a policy that describes this dataset.
        @param policy    a policy instance to write into.  If not provided
                           (default) a new one is created.
        @return Policy   the policy containing the description of this dataset.
        """
        if not policy:
            policy = Policy()
        if self.type:  policy.set("type", self.type)

        if self.ids:
            ids = Policy()
            policy.set("ids", ids)
            for id in self.ids.keys():
                ids.set(id, self.ids[id])

        if self.path:  policy.set("path", self.path)
        if self.valid is not None:  policy.set("valid", self.valid)

        return policy

    def _policy_(self):
        return self.toPolicy()

    @staticmethod
    def fromPolicy(policy):
        """
        unserialize a dataset description from a policy
        """
        valid = type = ids = path = None

        if policy.exists("type"):  type  = policy.getString("type")
        if policy.exists("path"):  path  = policy.getString("path")
        if policy.exists("valid"): valid = policy.getBool("valid")
        if policy.exists("ids"):  
            idp = policy.getPolicy("ids")
            ids = {}
            for name in idp.paramNames():
                ids[name] = idp.get(name)

        return Dataset(type, path, valid, ids)


        
