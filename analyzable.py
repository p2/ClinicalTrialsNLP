#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#	Representing a codifiable object property
#
#	2013-10-09	Created by Pascal Pfiffner
#

import uuid
import logging
from datetime import datetime


class Analyzable (object):
	""" Representing a codifiable object property. """
	
	
	def __init__(self, obj, prop):
		self.uuid = uuid.uuid4()
		self.object = obj
		self.prop = prop
		self._waiting_for_nlp = set()		# set of NLP engine names to be run
		self.codified = None				# dictionary with codes and dates per NLP name
	
	
	def waiting_for_nlp(self, nlp_name):
		return nlp_name in self._waiting_for_nlp
	
	
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
		if self.object is None:
			raise Exception("Must set 'object' before running NLP analysis")
		if self.prop is None:
			raise Exception("Must set 'prop' before running NLP analysis")
		
		# try to get text from the object's property
		text = getattr(self.object, self.prop)
		if text is None or 0 == len(text):
			return
		
		if not isinstance(text, basestring):
			try:
				text = text['textblock']
			except Exception, e:
				pass
		
		if not isinstance(text, basestring):
			logging.error('The property "%s" is not a string, cannot analyze' % self.prop)
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

