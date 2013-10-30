#!/usr/bin/env python
#
#	cTAKES and RegEx wizardry
#
#	2012-12-14	Created by Pascal Pfiffner
#

import os
import re
import logging


class NLPProcessing (object):
	""" Abstract base class for handling NLP pipelines. """
	
	def __init__(self):
		self.name = 'nlp'
		self.bin = '.'
		self.root = None
		self.cleanup = True
		self.did_prepare = False
	
	
	# -------------------------------------------------------------------------- Preparations
	def set_relative_root(self, directory):
		self.root = os.path.abspath(directory if directory is not None else '.')
	
	def prepare(self):
		""" Performs steps necessary to setup the pipeline, such as creating
		input and output directories or pipes. """
		self._prepare()
		self.did_prepare = True
	
	def _prepare(self):
		if self.root is None:
			raise Exception("No root directory defined for NLP process %s" % self.name)
		
		if not os.path.exists(self.root):
			os.mkdir(self.root)
		
		self._create_directories_if_needed()
		
		if not os.path.exists(self.root):
			raise Exception("Failed to create root directory for NLP process %s" % self.name)
	
	def _create_directories_if_needed(self):
		""" Override to create directories needed to run the pipeline. """
		pass
	
	
	# -------------------------------------------------------------------------- Running
	def run(self):
		""" Runs the NLP pipeline, raises an exception on error. """
		if not self.did_prepare:
			self.prepare()
		self._run()
	
	def _run(self):
		""" Internal use, subclasses should override this method since it is
		called after necessary preparation has been performed. """
		raise Exception("Cannot run an abstract NLP pipeline class instance")
	
	def write_input(self, text, filename):
		if not self.did_prepare:
			self.prepare()
		
		return self._write_input(text, filename)

	def _write_input(self, text, filename):
		return False
	
	def parse_output(self, filename, **kwargs):
		if not self.did_prepare:
			self.prepare()
		
		return self._parse_output(filename, **kwargs)
	
	def _parse_output(self, filename, **kwargs):
		""" return a dictionary (or None) like:
		{ 'snomed': [1, 2, 2], 'rxnorm': [4, 5, 6] }
		"""
		return None


# ------------------------------------------------------------------------------ Helper Functions
def split_inclusion_exclusion(string):
	""" Returns a tuple of lists describing inclusion and exclusion criteria.
	"""
	
	if not string or len(string) < 1:
		raise Exception('No string given')
	
	# split on newlines
	rows = re.compile(r'(?:\n\s*){2,}').split(string)
	
	# loop all rows
	missed = []
	inc = []
	exc = []
	at_inc = False
	at_exc = False
	
	for string in rows:
		if len(string) < 1 or 'none' == string:
			continue
		
		clean = re.sub(r'[\n\s]+', ' ', string).strip()
		
		# detect switching to inclusion criteria
		# exclusion criteria sometimes say "None if patients fulfill inclusion
		# criteria.", try to avoid detecting that as header!
		if re.search(r'^[^\w]*inclusion criteria', clean, re.IGNORECASE) is not None \
			and re.search(r'exclusion', clean, re.IGNORECASE) is None:
			at_inc = True
			at_exc = False
		
		# detect switching to exclusion criteria
		elif re.search(r'exclusion criteria', clean, re.IGNORECASE) is not None \
			and re.search(r'inclusion', clean, re.IGNORECASE) is None:
			at_inc = False
			at_exc = True
		
		# assign accordingly
		elif at_inc:
			inc.append(clean)
		elif at_exc:
			exc.append(clean)
		else:
			missed.append(clean)
	
	# if there was no inclusion/exclusion split, we assume the text describes inclusion criteria
	if len(inc) < 1 or len(exc) < 1:
		logging.debug("No explicit separation of inclusion/exclusion criteria found, assuming the text to describe inclusion criteria")
		inc.extend(missed)
		exc = []
	
	return (inc, exc)


def list_to_sentences(string):
	""" Splits text at newlines and puts it back together after stripping new-
	lines and enumeration symbols, joined by a period.
	"""
	if string is None:
		return None
	
	lines = string.splitlines()
	
	curr = ''
	processed = []
	for line in lines:
		if 0 == len(line):
			if curr:
				processed.append(re.sub(r'\.\s*$', '', curr))
			curr = ''
		
		elif not curr or 0 == len(curr):
			curr = line.strip()
		
		# new line item? true when it starts with "-", "1." or "1)" (with
		# optional dash) or if the indent level is less than before (simple
		# whitespace count)
		elif re.match(r'^\s*-\s+', line) \
			or re.match(r'\s*\d+\.\s+', line) \
			or re.match(r'^\s*(-\s*)?\d+\)\s+', line):
			
			if curr:
				processed.append(re.sub(r'\.\s*$', '', curr))
			curr = line
		else:
			curr = '%s %s' % (curr, line.strip()) if curr else line.strip()
	
	if curr:
		processed.append(re.sub(r'\.\s*$', '', curr))
	
	sentences = '. '.join(processed) if len(processed) > 0 else ''
	if len(sentences) > 0:
		sentences += '.'
	
	return sentences


def list_trim(string):
	""" Trim text phases that are part of the string because the string was
	pulled off of a list, e.g. a leading "-" or "1."
	"""
	
	string.strip()
	string = re.sub('\s+', ' ', string)						# multi-whitespace
	string = re.sub('^-\s+', '', string, count=1)			# leading "-"
	string = re.sub('^\d+\.\s+', '', string, count=1)			# leading "1."
	string = re.sub('^(-\s*)?\d+\)\s+', '', string, count=1)		# leading "1)" with optional dash
	
	return string

