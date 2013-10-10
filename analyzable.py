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
		self.codified = None				# dictionary with codes per NLP name
		self.last_codified = None
	
	
	def waiting_for_nlp(self, nlp_name):
		return nlp_name in self._waiting_for_nlp
	
	
	# -------------------------------------------------------------------------- Codifying
	def codify(self, nlp_engines):
		""" Handle codification by the given nlp_engines, instances of
		NLPProcessing and its subclasses. """
		
		for nlp in nlp_engines:
			if not self.parse_nlp_output(nlp):
				self.write_nlp_input(nlp)
	
	def write_nlp_input(self, nlp_engine):
		if self.object is None:
			raise Exception("Must set 'object' before running NLP analysis")
		if self.prop is None:
			raise Exception("Must set 'prop' before running NLP analysis")
		
		# try to get text from the object's property
		text = getattr(self.object, self.prop)
		if text is None:
			return
		
		if not isinstance(text, basestring):
			try:
				text = text['textblock']
			except Exception, e:
				pass
		
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
		
		# got cTAKES data
		result = self.codified or {}
		if 'ctakes' == nlp_engine.name:
			if 'snomed' in ret and ret['snomed'] is not None:
				result['snomed_ctakes'] = ret['snomed']
			if 'cui' in ret and ret['cui'] is not None:
				result['cui_ctakes'] = ret['cui']
			if 'rxnorm' in ret and ret['rxnorm'] is not None:
				result['rxnorm_ctakes'] = ret['rxnorm']
		
		# got MetaMap data
		elif 'metamap' == nlp_engine.name:
			if 'cui' in ret and ret['cui'] is not None:
				result['cui_metamap'] = ret['cui']
		
		self.codified = result if len(result) > 0 else None
		
		# end
		self.last_codified = datetime.now()
		if self._waiting_for_nlp and nlp_engine.name in self._waiting_for_nlp:
			self._waiting_for_nlp.remove(nlp_engine.name)
		
		return True

