#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#	Representing an object that can store to SQLite
#
#	2013-04-16	Created by Pascal Pfiffner
#

import logging

from sqlite import SQLite


class DBObject (object):
	""" A superclass for objects that can dehydrate to and hydrate from SQLite.
	
	Very crude and basic for the time being, but still takes away much of the
	cruft for subclasses.
	"""
	
	sqlite_default_db = 'databases/storage.db'
	sqlite_handle = None
	sqlite_must_commit = False
	
	table_name = None
	table_key = None
	
	def __init__(self):
		self.id = None
		self.hydrated = False
	
	
	# -------------------------------------------------------------------------- Dehydration
	def should_insert(self):
		""" Return True if the receiver should be inserted (i.e. is not already
		in the db). """
		return False
	
	def will_insert(self):
		""" Called before the insert query is performed, you can use this as a
		hook. """
		pass
	
	def insert_tuple(self):
		""" Cheap solution for now: return the INSERT sql as first and a list
		of values as second object. """
		return None, None
	
	def did_insert(self):
		pass
	
	def insert(self):
		""" Runs an INSERT query for the receiver.
		This method will not check with "should_insert()"! """
		self.will_insert()
		
		sql, params = self.insert_tuple()
		if sql is None or params is None:
			return False
		
		cls = self.__class__
		cls.sqlite_assure_handle()
		self.id = cls.sqlite_handle.executeInsert(sql, params)
		cls.sqlite_must_commit = True
		self.did_insert()
		
		return True
	
	
	def should_update(self):
		return True
	
	def update_tuple(self):
		""" Cheap solution for now: return the UPDATE sql as first and a list
		of values as second object. """
		return None, None
	
	def update(self):
		""" Runs the UPDATE query on the receiver. """
		
		sql, params = self.update_tuple()
		if sql is None or params is None:
			return False
		
		cls = self.__class__
		cls.sqlite_assure_handle()
		if cls.sqlite_handle.execute(sql, params):
			cls.sqlite_must_commit = True
			self.hydrated = True
			return True
		
		return False
	
	def did_store(self):
		""" Called after a successful call to self.store(). """
		pass
	
	def store(self):
		""" Stores the receiver's data to SQLite. You must MANUALLY COMMIT!
		"""
		
		# do we need to insert first?
		if self.should_insert() and not self.insert():
			logging.warning("Failed to INSERT %s" % self)
		
		# perform the update
		if self.should_update() and not self.update():
			logging.warning("Failed to UPDATE %s" % self)
			return False
		
		self.did_store()
		return True
	
	
	# -------------------------------------------------------------------------- Hydration
	def load(self, force=False):
		""" Hydrate from database. """
		pass
	
	def from_db(self, data):
		""" Fill from an SQLite-retrieved list. """
		pass
	
	
	# -------------------------------------------------------------------------- SQLite Methods
	def sqlite_execute(self, sql, params):
		""" Executes the given SQL statement with the given parameters.
		Returns True on success, False otherwise. """
		
		cls = self.__class__
		cls.sqlite_assure_handle()
		if cls.sqlite_handle.execute(sql, params):
			cls.sqlite_must_commit = True
			self.hydrated = True
			return True
		
		return False
	
	@classmethod
	def sqlite_select(cls, sql, params):
		""" Executes the SQL statement and returns the response. You can use
		this method in an iterator. """
		
		cls.sqlite_assure_handle()
		return cls.sqlite_handle.execute(sql, params)
	
	@classmethod
	def sqlite_select_one(cls, sql, params):
		""" Executes the SQL statement and returns the first response row.
		"""
		
		cls.sqlite_assure_handle()
		return cls.sqlite_handle.executeOne(sql, params)
	
	@classmethod
	def add_index(cls, table_column):
		""" Adds an index for the given table column if there is none.
		"""
		if table_column is None:
			return
		
		cls.sqlite_assure_handle()
		idx_name = "%s_index" % table_column
		cls.sqlite_handle.execute("CREATE INDEX IF NOT EXISTS %s ON %s (%s)" % (idx_name, cls.table_name, table_column))
	
	
	# -------------------------------------------------------------------------- Class Methods
	@classmethod
	def sqlite_assure_handle(cls):
		if cls.sqlite_handle is None:
			cls.sqlite_handle = SQLite.get(cls.sqlite_default_db)
	
	@classmethod
	def sqlite_release_handle(cls):
		cls.sqlite_handle = None
	
	@classmethod
	def sqlite_commit_if_needed(cls):
		""" Commits to SQLite if the flag had been set. """
		if cls.sqlite_handle is None:
			return
		
		if cls.sqlite_must_commit:
			cls.sqlite_must_commit = False
			cls.sqlite_handle.commit()
	
	
	# -------------------------------------------------------------------------- Table Setup
	@classmethod
	def table_structure(cls):
		""" Return the table structure here. """
		return None
	
	@classmethod
	def setup_tables(cls, db_path=None):
		if db_path is not None:
			cls.sqlite_default_db = db_path
		
		struct = cls.table_structure()
		if struct is None:
			return False
		
		cls.sqlite_assure_handle()
		if cls.sqlite_handle.create(cls.table_name, struct):
			cls.did_setup_tables(db_path)
	
	@classmethod
	def did_setup_tables(cls, db_path):
		pass
	
	# call the table setup to be sure it was set up
	# SubClass.setup_tables()
	
	
