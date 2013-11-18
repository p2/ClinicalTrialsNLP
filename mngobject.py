#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#	Superclass for objects stored in MongoDB
#
#	2013-07-10	Created by Pascal Pfiffner
#

import logging
import collections

from pymongo import MongoClient


class MNGObject (object):
	""" Superclass for an object stored in a MongoDB collection. """
	
	def __init__(self, id=None):
		self.id = id
		self.doc = None
		self.loaded = False
	
	
	# -------------------------------------------------------------------------- MongoDB
	database_uri = "mongodb://localhost:27017"
	
	# the MongoDB database may be 'None', in which case the default db will be
	# used, and if that doesn't work it will fall back to use 'default'
	database_name = None
	
	# the MongoDB collection that holds documents of this class
	collection_name = None
	
	_collection = None
	
	@classmethod
	def collection(cls):
		""" Returns a Mongo Collection object, creating it if necessary. """
		if cls._collection is None:
			if not cls.collection_name:
				raise Exception("No collection has been set for %s" % cls)
			
			client = MongoClient(cls.database_uri)
			if cls.database_name is None:
				try:
					db = client.get_default_database()
				except Exception, e:
					logging.debug("Failed to get default database: %s" % e)
					db = client['default']
			else:
				db = client[cls.database_name]
			cls._collection = db[cls.collection_name]
		
		return cls._collection
	
	@classmethod
	def test_connection(cls):
		""" Tests the database by inserting, retrieving and deleting a document.
		"""
		old_coll = cls.collection_name
		cls.collection_name = 'foo'
		
		obj = MNGObject()
		obj.doc = {
			'title': "This is a connection test document"
		}
		
		ret = None
		
		# try storing
		try:
			obj.store()
			
			# try loading
			sec = MNGObject(obj.id)
			try:
				sec.load()
				
				# compare titles
				t1 = obj.doc.get('title') if obj.doc else None
				t2 = sec.doc.get('title') if sec.doc else None
				if t1 == t2:
					
					# try removing
					try:
						if not sec.remove():
							raise Exception('failed to remove')
					except Exception, e:
						ret = "TEST FAILED with remove() exception: %s" % e
				else:
					ret = "TEST FAILED, insertion and retrieval do not match (%s != %s)" % (t1, t2)
			except Exception, e:
				ret = "TEST FAILED with load() exception: %s" % e
		except Exception, e:
			ret = "TEST FAILED with store() exception: %s" % e

		
		# clean up
		try:
			cls._collection.drop()
			cls._collection = None
		except:
			logging.error("Failed to drop collection: %s" % e)
		
		cls.connection_name = old_coll
		
		return ret
	
	
	# -------------------------------------------------------------------------- Document Manipulation
	def ensure_doc_id(self):
		had_doc = True
		if self.doc is None:
			had_doc = False
			self.doc = {}
		
		if self.id:
			self.doc['_id'] = self.id
		elif had_doc:
			self.id = self.doc.get('_id')
			if self.id is None:
				self.id = self.doc.get('id')
				self.doc['_id'] = self.id
	
	def replace_with(self, json):
		""" Replaces the document tree with the given JSON tree.
		
		The document id is set from the receiver's id if it's there, otherwise
		it's being searched in the doc in this order:
		- if self.id is not None, the doc's "_id" will be set to self.id
		- if doc["_id"] is present, this becomes self.id
		- if doc["id"] is present, this becomes self.id and is set as the
		  docs "_id"
		"""
		if not self.loaded:
			self.load()
		
		self.doc = json
		self.loaded = True
		
		# set or update our id
		self.ensure_doc_id()
		self.did_update_doc()
	
	def update_with(self, json):
		""" Updates the document tree by merging it with the given JSON tree.
		
		The id of the document is automatically set in this order:
		- if self.id is not None, the doc's "_id" will be set to self.id
		- if doc["_id"] is present, this becomes self.id
		- if doc["id"] is present, this becomes self.id and is set as the
		  docs "_id"
		"""
		
		if not self.loaded:
			self.load()
		
		# set or update contents
		if self.doc is None:
			self.doc = json
		else:
			self.doc = deepUpdate(self.doc, json)
		self.loaded = True
		
		# set or update our id
		self.ensure_doc_id()
		self.did_update_doc()
	
	def did_update_doc(self):
		""" Called when self.doc has been changed programmatically (i.e. NOT
		after loading from database).
		
		You can call this manually if you directly assign self.doc and want
		this to trigger. The default implementation does nothing.
		"""
		pass
	
	def update_subtree(self, keypath, tree):
		assert False, "Not implemented"
	
	def replace_subtree(self, keypath, tree):
		""" replaces the existing tree at keypath with the new tree. """
		
		if not self.loaded and self.id:
			self.load()
		
		self.ensure_doc_id()
		self.doc = replaceSubtree(self.doc, keypath, tree)
		self.loaded = True
	
	
	# -------------------------------------------------------------------------- Dehydration
	def store(self, subtree=None):
		""" Stores the receiver's data to the collection, letting Mongo decide
		between an insert and an update.
		If "subtree" is not None, an update is forced only on the given subtree
		which should have the format: {'keypath': value}. """
		
		# throw up if there is no content and we're not saving a subtree
		if self.doc is None and subtree is None:
			raise Exception("This object does not have content")
		
		cls = self.__class__
		
		# update if there's a subtree, otherwise use "save"
		if subtree is not None:
			if self.id is None:
				raise Exception("No id is set, cannot update subtree %s" % subtree)
			res = cls.collection().update({"_id": self.id}, {"$set": subtree})
			if res is not None:
				if res.get('err'):
					logging.warning("Error while saving subtree: %s" % res.get('err'))
				
				# instead of loading again, would be nice to update self.doc
				# appropriately
				self.doc = None
				self.load()
		else:
			self.id = cls.collection().save(self.doc, manipulate=True)
		
		self.did_store()
		
		return True
	
	def did_store(self):
		""" Called after a successful call to "store". """
		pass
	
	
	# -------------------------------------------------------------------------- Hydration
	def load(self, force=False):
		""" Hydrate from database, if the instance has an id.
		If the document already has an in-memory representation, data loaded
		from database will be superseeded by the in-memory properties unless
		"force" is set to True, which will make all in-memory data to be
		discarded.
		
		Arguments:
		force -- if True will discard any in-memory changes to self.doc
		"""
		
		if self.id is None:
			return
		
		found = self.__class__.collection().find_one({"_id": self.id})
		if found is not None:
			if force or self.doc is None:
				self.doc = found
			else:
				self.doc = deepUpdate(found, self.doc)
		
		self.loaded = True
	
	
	# -------------------------------------------------------------------------- Multiple
	@classmethod
	def retrieve(cls, id_list=[]):
		""" Retrieves multiple documents by id. """
		
		found = []
		for document in cls.collection().find({"_id": {"$in": id_list}}):
			obj = cls()
			obj.update_with(document)
			
			found.append(obj)
		
		return found
	
	
	# -------------------------------------------------------------------------- Deletion
	def remove(self):
		""" Delete from database. """
		
		if self.id is None:
			raise Exception("This object does not have an id, cannot remove")
		
		ret = self.__class__.collection().remove(spec_or_id=self.id)
		return ret.get('err') is None if ret else False



def deepUpdate(d, u):
	""" Deep merges two dictionaries, overwriting "d"s values with "u"s where
	present. """
	if u is None:
		return d
	
	# if we have "u" and "d" is not a mapping object, we overwrite it with "u"
	if d is None or not isinstance(d, collections.Mapping):
		return u
	
	# iterate over keys and values and update
	for k, v in u.iteritems():
		if isinstance(v, collections.Mapping):
			old = d.get(k)
			d[k] = deepUpdate(old, v) if old else v
		else:
			d[k] = u[k]
	
	return d

def deleteSubtree(tree, keypath):
	""" Deletes the content at keypath. """
	if not keypath:
		raise Exception("You must provide a keypath")
	
	existing = tree
	path = keypath.split('.')
	while len(path) > 1:
		p = path.pop(0)
		existing = existing.get(p)
		
		# if we don't have a tree to update it's not there anyway, go home
		if existing is None:
			return tree
	
	del existing[path[0]]
	
	return tree


def replaceSubtree(tree, keypath, json):
	""" Replaces or creates a subtree at keypath. """
	if not keypath:
		raise Exception("You must provide a keypath")
	if json is None:
		return deleteSubtree(tree, keypath)
	
	existing = tree or {}
	path = keypath.split('.')
	while len(path) > 1:
		p = path.pop(0)
		previous = existing
		existing = existing.get(p)
		if existing is None:
			existing = {}
			previous[p] = existing
	
	if existing is None:
		existing = {}
	existing[path[0]] = json
	
	return tree


if '__main__' == __name__:
	a = {'a': 1, 'b': 1,	'c': {'ca': 1, 'cb': 1,						'cc': {'cca': 1, 'ccb': 1}},				'e': {'ea': 1}}
	b = {'a': 2,			'c': {'ca': 2, 'cb': {'cba': 2, 'cbb': 2},		'cd': {'cda': 2, 'cdb': 2, 'cdc': 2}},	'e': 2}
	
	print "replaceSubtree()"
	print "before   ", a
	print "replace 1", replaceSubtree(a, 'c.ca', 3)
	print "replace 2", replaceSubtree(a, 'c.cc.cca', 3)
	print "replace 3", replaceSubtree(a, 'c.ce.cea', 3)
	print
	print "deleteSubtree()"
	print "before  ", a
	print "delete 1", deleteSubtree(a, 'c.ce.cea')
	print "delete 2", deleteSubtree(a, 'd.da.dda')
	print
	print "deepUpdate(a, b)"
	print "a: ", a
	print "b: ", b
	print "-> ", deepUpdate(a, b)
	
