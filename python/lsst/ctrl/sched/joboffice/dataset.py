"""
classes for describing datasets.  
"""
from __future__ import with_statement

from lsst.pex.policy import Policy
from lsst.ctrl.sched.blackboard.base import _AbstractBase

import os

class Dataset(object):
    """
    a description of a dataset.  

    This description is characterized by a dataset type name and a 
    set of identifiers.  These attributes are access via public member 
    variables 'type' (a string) and ids (a dictionary), respectively.
    """

    def __init__(self, type, path=None, ids=None, **kw):
        """
        create the dataset
        @param type    the dataset type name
        @param path    a filesystem pathname to the file.  If None, the 
                         path is not known/applicable
        @param ids     a dictionary of identifiers, mapping names to values.
                         the type of the identifier is context specific.
        @param *       additional named parameters are taken as 
                         identifiers to be set with the given values
        """
        self.type = type
        self.path = path

        self.ids = None
        if ids:
            self.ids = dict(ids)
        if kw:
            if self.ids is None:
                self.ids = {}
            for key in kw.keys():
                self.ids[key] = kw[key]

    @staticmethod
    def fromPolicy(policy):
        """
        unserialize a dataset description from a policy
        """
        type = ids = path = None

        if policy.exists("type"):  type = policy.getString("type")
        if policy.exists("path"):  path = policy.getString("path")
        if policy.exists("ids"):  
            idp = policy.getPolicy("ids")
            ids = {}
            for name in idp.paramNames():
                ids[name] = idp.get(name)

        return Dataset(type, path, ids)


        
