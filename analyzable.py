#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#	Representing a codifiable object property
#
#	2013-10-09	Created by Pascal Pfiffner
#

import uuid
import string
import logging
from datetime import datetime


class Analyzable (object):
	""" Representing a codifiable object property. """
	
	
	def __init__(self, obj, keypath):
		self._uuid = None
		self.object = obj
		self.keypath = keypath
		self._waiting_for_nlp = set()		# set of NLP engine names to be run
		self.codified = None				# dictionary with codes and dates per NLP name
	
	
	@property
	def uuid(self):
		if not self._uuid:
			self._uuid = uuid.uuid4()
		return self._uuid
	
	
	def waiting_for_nlp(self, nlp_name):
		return nlp_name in self._waiting_for_nlp
	
	def extract_string(self):
		""" Handles the property path until a string is found.
		Arrays are detected and the values are joined with full sentence stops
		if the last character of the string is not punctuation. Checks for
		"textblock" items automatically if the final object is not a string.
		Raises if either "object" or "keypath" is missing.
		"""
		if self.object is None:
			raise Exception("Must set 'object' before running NLP analysis")
		if self.keypath is None:
			raise Exception("Must set 'keypath' before running NLP analysis")
		
		# get string objects
		objs = _analyzable_objects_at_keypath(self.object, self.keypath)
		strings = []
		for obj in objs:
			if isinstance(obj, dict):
				obj = obj.get('textblock', '')
			if not isinstance(obj, basestring):
				logging.error('The property at "%s" is not a string, cannot analyze' % self.keypath)
				return None
			
			strings.append(obj)
		
		# collapse into string
		if 1 == len(strings):
			return strings[0]
		
		punctuated = []
		for mystr in strings:
			if len(mystr) > 0:
				if mystr[-1] not in string.punctuation:
					mystr += '.'
				punctuated.append(mystr)
		
		return ' '.join(punctuated)
	
	
	# -------------------------------------------------------------------------- Codifying
	def codify(self, nlp_engines, force=False):
		""" Handle codification by the given nlp_engines, instances of
		NLPProcessing and its subclasses.
		
		Returns a dictionary "nlp: codes" for the newly codified NLP pipelines.
		"""
		
		all_new = {}
		for nlp in nlp_engines:
			if force or not self.codified or not self.codified.get(nlp.name):
				if self.parse_nlp_output(nlp):
					all_new[nlp.name] = self.codified.get(nlp.name)
				else:
					self.write_nlp_input(nlp)
		
		return all_new if len(all_new) > 0 else None
	
	def write_nlp_input(self, nlp_engine):
		""" Get the string we want to analyze and tells the NLP engine to write
		it to their input file. """
		
		text = self.extract_string()
		if text is None or 0 == len(text):
			return
		
		# write to file and set waiting flag
		if nlp_engine.write_input(unicode(text), '%s.txt' % self.uuid):
			self._waiting_for_nlp.add(nlp_engine.name)
	
	def parse_nlp_output(self, nlp_engine):
		""" Returning False from this method will result in 'write_nlp_input' to
		be called. """
		
		# parse our file; if it doesn't return a result we'll return False
		filename = '%s.txt' % self.uuid
		ret = nlp_engine.parse_output(filename, filter_sources=True)
		if ret is None:
			return False
		
		# remember codified data -- "ret" should be a dictionary
		result_all = self.codified or {}
		result = result_all.get(nlp_engine.name, {})
		result_codes = result.get('codes', {})
		
		# iterate to not override existing but differently keyed codes
		for typ, val in ret.iteritems():
			if val is not None:
				result_codes[typ] = val
		
		result['date'] = datetime.now()
		result['codes'] = result_codes
		
		result_all[nlp_engine.name] = result if len(result_codes) > 0 else None
		self.codified = result_all
		
		# end
		if self._waiting_for_nlp and nlp_engine.name in self._waiting_for_nlp:
			self._waiting_for_nlp.remove(nlp_engine.name)
		
		return True


def _analyzable_objects_at_keypath(obj, keypath):
	""" Always returs an array (or None). """
	assert(keypath)
	path = keypath.split('.')
	objs = [obj]
	while len(path) > 0:
		p = path.pop(0)
		new_objs = []
		for this_obj in objs:
			this_ret = None
			try:
				this_ret = getattr(this_obj, p)
			except AttributeError as e:
				try:
					this_ret = this_obj.get(p)
				except:
					pass
			except:
				pass
			
			if this_ret is not None:
				new_objs.extend(this_ret if isinstance(this_ret, list) else [this_ret])
		
		objs = new_objs
	
	return objs
		


# some tests
if '__main__' == __name__:
	dic = {
		'foo': "Hello",
		'bar': [
			"multiple strings",
			"in an array"
		],
		'hat': [{
			'item': "Multiple sentences"
		},
		{
			'item': "Buried in an array of dictionaries"
		},
		{
			'item': "Quite crazy!"
		}],
		'bat': [{
			'arr': [{
				'nested': "Quite deeply nested"
			},
			{
				'nested': "Don't you think?"
			},
			{
				'nested': "I wonder if this works"
			}]
		},
		{
			'arr': [{
				'nested': "This is crazy!"
			},
			{
				'nested': "Running out of sentences"
			},
			{
				'nested': "Send Help!"
			}]
		}]
	}
	
	# debug
	print("->  This will be returned")
	print(_analyzable_objects_at_keypath(dic, 'foo'))
	print(_analyzable_objects_at_keypath(dic, 'bar'))
	print(_analyzable_objects_at_keypath(dic, 'hat.item'))
	print(_analyzable_objects_at_keypath(dic, 'bat.arr.nested'))
	print("->  Starting assert tests")
	
	# simple
	a = Analyzable(dic, 'foo')
	assert("Hello" == a.extract_string())
	
	# simple array
	a.keypath = 'bar'
	assert("multiple strings. in an array." == a.extract_string())
	
	# nested array
	a.keypath = 'hat.item'
	assert("Multiple sentences. Buried in an array of dictionaries. Quite crazy!" == a.extract_string())
	
	# nested nested
	a.keypath = 'bat.arr.nested'
	assert("Quite deeply nested. Don't you think? I wonder if this works. This is crazy! Running out of sentences. Send Help!" == a.extract_string())
	
	print("->  Done")

