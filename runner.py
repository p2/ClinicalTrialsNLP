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
from subprocess import call

from ClinicalTrials.study import Study
from ClinicalTrials.lillycoi import LillyCOI
from ClinicalTrials.umls import UMLS


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
		
		self.run_ctakes = False
		self.run_metamap = False
		
		self.condition = None
		self.term = None
		self.found_studies = None
		
		self._status = None
		self._done = False
		self.in_background = False
		self.log_status = False
		self.worker = None
	
	
	# -------------------------------------------------------------------------- Running
	def run(self, fields=None):
		""" Start running. """
		if self.in_background:
			worker = Thread(target=self._run, kwargs={'fields': fields})
			worker.start()
		else:
			self._run(fields)
	
	
	def _run(self, fields=None):
		""" Runs the whole toolchain.
		Currently writes all status to a file associated with run_id. If the
		first word in that file is "error", the process is assumed to have
		stopped. If it is "done" the work here is done.
		"""
		
		# check prerequisites
		if self.condition is None and self.term is None:
			raise Exception("No condition and no term provided")
		
		self.assure_run_directory()
		self.status = "Searching for %s trials..." % self.condition if self.condition is not None else self.term
		
		# setup
		Study.sqlite_release_handle()
		UMLS.check_databases(True)
		
		db_path = os.path.join(self.run_dir, 'storage.db')
		Study.setup_tables(db_path)
		if self.run_ctakes:
			Study.setup_ctakes({'root': self.run_dir, 'cleanup': False})
		if self.run_metamap:
			Study.setup_metamap({'root': self.run_dir, 'cleanup': False})
		
		# search for studies
		if fields is None:
			fields = ['id', 'eligibility']
		
		self.status = "Fetching %s trials..." % self.condition if self.condition is not None else self.term
		
		# anonymous callback for progress reporting
		def cb(inst, progress):
			if progress > 0:
				self.status = "Fetching, %d%% done..." % (100 * progress)
		
		lilly = LillyCOI()
		if self.condition is not None:
			self.found_studies = lilly.search_for_condition(self.condition, True, fields, cb)
		else:
			self.found_studies = lilly.search_for_term(self.term, True, fields, cb)
		
		# process all studies
		run_ctakes = False
		run_metamap = False
		ncts = []
		for study in self.found_studies:
			ncts.append(study.nct)
			self.status = "Processing %d of %d..." % (len(ncts), len(self.found_studies))
			
			study.load()
			study.process_eligibility_from_text()
			study.codify_eligibility()
			if study.waiting_for_nlp('ctakes'):
				run_ctakes = True
			if study.waiting_for_nlp('metamap'):
				run_metamap = True
			study.store()
		
		self.write_ncts(ncts)
		success = True
		
		# run cTakes if needed
		if run_ctakes:
			self.status = "Running cTakes for %d trials (this will take a while)..." % len(ncts)
			success_ct = True
			
			try:
				if call(['./run_ctakes.sh', self.run_dir]) > 0:
					self.status = 'Error running cTakes'
					success_ct = False
			except Exception, e:
				self.status = 'Error running cTakes: %s' % e
				success_ct = False
			
			# make sure we got all criteria
			if success_ct:
				for study in self.found_studies:
					study.codify_eligibility()
					study.store()
			else:
				success = False
		
		# run MetaMap if needed
		if run_metamap:
			self.status = "Running MetaMap for %d trials (this will take a while)..." % len(ncts)
			success_mm = True
			
			try:
				if call(['./run_metamap.sh', self.run_dir]) > 0:
					self.status = 'Error running MetaMap'
					success_mm = False
			except Exception, e:
				self.status = 'Error running MetaMap: %s' % e
				success_mm = False
			
			# make sure we got all criteria
			if success_mm:
				for study in self.found_studies:
					study.codify_eligibility()
					study.store()
			else:
				success = False
		
		Study.sqlite_commit_if_needed()
		Study.sqlite_release_handle()
		if success:
			self.status = 'done'
	
	
	# -------------------------------------------------------------------------- Run Directory
	def assure_run_directory(self):
		if self.run_dir is None:
			raise Exception("No run directory defined for runner %s" % self.name)
		
		if not os.path.exists(self.run_dir):
			os.mkdir(self.run_dir)
			if self.run_ctakes:
				os.mkdir(os.path.join(self.run_dir, 'ctakes_input'))
				os.mkdir(os.path.join(self.run_dir, 'ctakes_output'))
			if self.run_metamap:
				os.mkdir(os.path.join(self.run_dir, 'metamap_input'))
				os.mkdir(os.path.join(self.run_dir, 'metamap_output'))
		
		if not os.path.exists(self.run_dir):
			raise Exception("Failed to create run directory for runner %s" % self.name)
	
	
	# -------------------------------------------------------------------------- Status
	@property
	def name(self):
		if self._name is None:
			self._name = "find-%s" % (self.condition if self.condition is not None else self.term)
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
		if self.log_status:
			logging.info("%s: %s" % (self.name, status))
		
		self._status = status
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
	


