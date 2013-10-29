#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#	A class handling full data runs
#
#	2013-05-09	Created by Pascal Pfiffner
#


import os
import logging

from threading import Thread

from ClinicalTrials.study import Study
from ClinicalTrials.lillycoi import LillyCOI
from ClinicalTrials.umls import UMLS
from ClinicalTrials.ctakes import cTAKES
from ClinicalTrials.metamap import MetaMap
from ClinicalTrials.nltktags import NLTKTags


class Runner (object):
	""" An instance of this class can perform data runs.
	"""
	
	runs = {}
	
	
	@classmethod
	def get(cls, run_id):
		""" Returns the runner if we already have it, None otherwise. """
		if run_id is None:
			raise Exception("No run-id provided")
		
		return cls.runs.get(run_id)
		
		# create a new run and stick it in our dictionary
		run = cls(run_id)
		cls.runs[run_id] = run
		
		return run
	
	
	def __init__(self, run_id, run_dir):
		if run_id is None:
			raise Exception("No run-id provided")
		
		self.run_id = run_id
		self._name = None
		self.run_dir = run_dir
		self.__class__.runs[run_id] = self
		
		self.analyze_eligibility = True
		self.analyze_properties = None		# set of property names
		
		self.run_ctakes = False
		self.run_metamap = False
		self.run_nltktags = False
		
		self.condition = None
		self.term = None
		
		self._status = None
		self._done = False
		self.in_background = False
		self.worker = None
	
	
	# -------------------------------------------------------------------------- Running
	def run(self, fields=None, callback=None):
		""" Start running.
		Arguments you can specify:
		- fields: an array of field names that should be retrieved.
		- callback: a callback function to be run at the end. The first argument
		  to the function will be a bool indicating whether the run was
		  successful, the second argument is the array of trials found during
		  the run.
		"""
		if self.in_background:
			worker = Thread(target=self._run, kwargs={'fields': fields, 'callback': callback})
			worker.start()
		else:
			self._run(fields, callback)
	
	
	def _run(self, fields=None, callback=None):
		""" Runs the whole toolchain.
		Currently writes all status to a file associated with run_id. If the
		first word in that file is "error", the process is assumed to have
		stopped. If it is "done" the work here is done.
		"""
		
		# check prerequisites
		if self.analyze_eligibility is False and self.analyze_properties is None:
			raise Exception("Nothing is set to be analyzed (set 'analyze_eligibility' or 'analyze_properties')")
		if self.condition is None and self.term is None:
			raise Exception("No 'condition' and no 'term' provided")
		
		self.assure_run_directory()
		self.status = "Searching for %s trials..." % (self.condition if self.condition is not None else self.term)
		
		# setup UMLS
		UMLS.check_databases(True)
		
		# setup NLP pipelines
		nlp_pipelines = []
		if self.run_ctakes:
			nlp_ct = cTAKES({'root': self.run_dir, 'cleanup': True})
			nlp_ct.prepare()
			nlp_pipelines.append(nlp_ct)
		if self.run_metamap:
			nlp_mm = MetaMap({'root': self.run_dir, 'cleanup': True})
			nlp_mm.prepare()
			nlp_pipelines.append(nlp_mm)
		if self.run_nltktags:
			nlp_nltkt = NLTKTags({'root': self.run_dir, 'cleanup': False})
			nlp_nltkt.prepare()
			nlp_pipelines.append(nlp_nltkt)
		
		# anonymous callback for progress reporting
		def cb(inst, progress):
			if progress > 0:
				self.status = "Fetching, %d%% done..." % (100 * progress)
		
		# make sure we retrieve the properties that we want to analyze
		if self.analyze_properties:
			if fields is None:
				fields = []
			fields.extend(self.analyze_properties)
		
		# start the search
		self.status = "Fetching %s trials..." % (self.condition if self.condition is not None else self.term)
		
		lilly = LillyCOI()
		trials = []
		if self.condition is not None:
			trials = lilly.search_for_condition(self.condition, True, fields, cb)
		else:
			trials = lilly.search_for_term(self.term, True, fields, cb)
		
		# process found trials
		ncts = []
		nlp_to_run = set()
		for trial in trials:
			ncts.append(trial.nct)
			self.status = "Processing %d of %d..." % (len(ncts), len(trials))
			
			trial.nlp = nlp_pipelines
			trial.analyze_properties = self.analyze_properties
			
			try:
				trial.load()
				if self.analyze_eligibility:
					trial.codify_eligibility_lilly()
				trial.codify_analyzables(nlp_pipelines)
			
			except Exception, e:
				self.status = 'Error processing trial: %s' % e
				return
			
			nlp_to_run.update(trial.waiting_for_nlp)
			trial.store()
		
		self.write_ncts(ncts)
		success = True
		
		# run the needed NLP pipelines
		for nlp in nlp_pipelines:
			if nlp.name in nlp_to_run:
				self.status = "Running %s for %d trials (this will take a while)..." % (nlp.name, len(trials))
				try:
					nlp.run()
				except Exception, e:
					self.status = "Running %s failed: %s" % (nlp.name, str(e))
					success = False
					break
		
		# make sure we codified all criteria
		if success:
			for trial in trials:
				if self.analyze_eligibility:
					trial.codify_eligibility_lilly()
				trial.codify_analyzables(nlp_pipelines)
		
		# run the callback
		if callback is not None:
			self.status = "Running callback"
			callback(success, trials)
		
		if success:
			self.status = 'done'
	
	
	# -------------------------------------------------------------------------- Run Directory
	def assure_run_directory(self):
		if self.run_dir is None:
			raise Exception("No run directory defined for runner %s" % self.name)
		
		if not os.path.exists(self.run_dir):
			os.mkdir(self.run_dir)
		
		if not os.path.exists(self.run_dir):
			raise Exception("Failed to create run directory for runner %s" % self.name)
	
	
	# -------------------------------------------------------------------------- Status
	@property
	def name(self):
		if self._name is None:
			self._name = "find '%s'" % (self.condition if self.condition is not None else self.term)
		return self._name

	@property
	def status(self):
		if self._status is None:
			if not os.path.exists('%s.status' % self.run_id):
				return None
			
			with open('%s/%s.status' % (self.run_dir, self.run_id)) as handle:
				status = handle.readline()
				if status is not None:
					self._status = status.strip()
		
		return self._status
	
	@status.setter
	def status(self, status):
		logging.debug("%s: %s" % (self.name, status))
		self._status = status
		
		if self.in_background:
			with open('%s/%s.status' % (self.run_dir, self.run_id), 'w') as handle:
				handle.write(status)
	
	@property
	def done(self):
		return True if 'done' == self.status else False
	
	
	# -------------------------------------------------------------------------- Results
	def write_ncts(self, ncts):
		""" The "ncts" argument should be tuples of NCT and a reason on why it
		was filtered, or None if it was not filtered.
		Writes one NCT code with a colon and the filter reason (if any) per line. """
		
		filename = '%s/%s.ncts' % (self.run_dir, self.run_id)
		with open(filename, 'w') as handle:
			for nct in ncts:
				if type(nct) is not tuple:
					nct = (nct,)
				handle.write(':'.join(nct) + "\n")
	
	def get_ncts(self):
		""" Read the previously stored NCTs with their filtering reason (if any)
		and return them as a list of tuples. """
		filename = '%s/%s.ncts' % (self.run_dir, self.run_id)
		if not os.path.exists(filename):
			return None
		
		ncts = []
		with open(filename) as handle:
			for line in handle.readlines():
				tpl = tuple(line.strip().split(':', 2))
				ncts.append(tpl)
		
		return ncts

