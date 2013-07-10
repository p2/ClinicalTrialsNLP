#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#	Superclass for objects stored in MongoDB
#
#	2013-07-10	Created by Pascal Pfiffner
#

import logging
from pymongo import MongoClient


class MNGObject (object):
	""" Superclass for an object stored in a MongoDB collection. """
	
	def __init__(self, id=None):
		self.id = id
		self.doc = None
		self.loaded = False
	
	
	# -------------------------------------------------------------------------- MangoDB
	database_name = 'default'
	
	# the MongoDB collection that holds documents of this class
	collection_name = None
	_collection = None
	
	@classmethod
	def collection(cls):
		if cls._collection is None and cls.database_name and cls.collection_name:
			client = MongoClient()
			db = client[cls.database_name]
			cls._collection = db[cls.collection_name]
		
		return cls._collection
	
	
	# -------------------------------------------------------------------------- Document Manipulation
	def updateWith(self, json):
		""" Updates the document tree by merging it with the given JSON tree. """
		
		if not self.loaded:
			self.load()
		
		# set or update contents
		if self.doc is None:
			self.doc = json
		else:
			self.doc.update(json)
		
		# set or update our id
		if self.id:
			self.doc['_id'] = self.id
		else:
			self.id = self.doc.get('_id', self.doc.get('id'))
	
	
	# -------------------------------------------------------------------------- Dehydration
	def store(self, subtree=None):
		""" Stores the receiver's data to the collection, letting Mongo decide
		between an insert and an update.
		If "subtree" is not None, an update is forced only on the given subtree
		which should have the format: {'keypath': value}. """
		
		if self.doc is None:
			raise Exception("This object does not have content")
		
		cls = self.__class__
		if cls.collection() is None:
			raise Exception("No collection has been set for %s" % cls)
		
		# update if there's a subtree, otherwise use "save"
		if subtree is not None:
			if self.id is None:
				raise Exception("No id is set, cannot update %s" % subtree)
			cls.collection().update({"_id": self.id}, {"$set": subtree})
		else:
			self.id = cls.collection().save(self.doc, manipulate=True)
		
		self.did_store()
		
		return True
	
	def did_store(self):
		""" Called after a successful call to "store". """
		pass
	
	
	# -------------------------------------------------------------------------- Hydration
	def load(self, force=False):
		""" Hydrate from database, if the instance has an id. """
		
		if self.id is None:
			return
		
		cls = self.__class__
		if cls.collection() is None:
			raise Exception("No collection has been set for %s" % cls)
		
		self.doc = cls.collection().find_one({"_id": self.id})
		self.loaded = True

