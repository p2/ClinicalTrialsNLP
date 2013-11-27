#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#	A class handling full data runs.
#	Great profiling tool: pycallgraph
#	pycallgraph graphviz -- ./mypythonscript.py
#
#	2013-05-09	Created by Pascal Pfiffner
#


import os
import logging

from threading import Thread

from ClinicalTrials.sqlite import SQLite
from ClinicalTrials.trial import Trial
from ClinicalTrials.lillycoi import LillyCOI


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
	
	
	def __init__(self, run_id, run_dir):
		if run_id is None:
			raise Exception("No run-id provided")
		
		self.run_id = run_id
		self._name = None
		self.run_dir = run_dir
		self.sqlite_db = os.path.join(run_dir, 'runs.sqlite')
		self.__class__.runs[run_id] = self
		
		self.catch_exceptions = True		# useful to turn off for debugging
		
		self.nlp_pipelines = []
		self.discard_cached = False			# ignore cached codes
		self.analyze_keypaths = None		# set of keypaths (strings)
		
		self.condition = None
		self.term = None
		self.reference_location = None		# tuple (latitude, longitude)
		self.limit = None
		
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
		if self.condition is None and self.term is None:
			raise Exception("No 'condition' and no 'term' provided")
		
		self.assure_run_directory()
		self.status = "Searching for %s trials..." % (self.condition if self.condition is not None else self.term)
		
		# anonymous callback for progress reporting
		def cb(inst, progress):
			if progress > 0:
				self.status = "Fetching (%d%%)" % (100 * progress)
		
		# make sure we retrieve the properties that we want to analyze
		if self.analyze_keypaths:
			if fields is None:
				fields = []
			fields.extend(self.analyze_keypaths)
			fields.append('eligibility')
		
		# start the search
		self.status = "Fetching %s trials..." % (self.condition if self.condition is not None else self.term)
		
		lilly = LillyCOI()
		trials = []
		if self.condition is not None:
			trials = lilly.search_for_condition(self.condition, True, fields, cb)
		else:
			trials = lilly.search_for_term(self.term, True, fields, cb)
		
		if self.limit and len(trials) > self.limit:
			trials = trials[:self.limit]
		
		# process found trials
		self.status = "Processing..."
		sqlite = SQLite.get(self.sqlite_db)
		
		progress = 0
		progress_tot = len(trials)
		progress_each = max(5, progress_tot / 25)
		ncts = []
		num_nlp_trials = 0
		nlp_to_run = set()
		for trial in trials:
			ncts.append(trial.nct)
			trial.analyze_keypaths = self.analyze_keypaths
			
			if self.catch_exceptions:
				try:
					trial.codify_analyzables(self.nlp_pipelines, self.discard_cached)
				except Exception, e:
					self.status = 'Error processing trial: %s' % e
					return
			else:
				trial.codify_analyzables(self.nlp_pipelines, self.discard_cached)
			
			trial.store()
			self.write_trial(sqlite, trial)
			
			# make sure we run the NLP pipeline if needed
			to_run = trial.waiting_for_nlp(self.nlp_pipelines)
			if len(to_run) > 0:
				nlp_to_run.update(to_run)
				num_nlp_trials = num_nlp_trials + 1
			
			# progress
			progress = progress + 1
			if 0 == progress % progress_each:
				self.status = "Processing (%d %%)" % (float(progress) / progress_tot * 100)
		
		sqlite.commit()
		
		# run the needed NLP pipelines
		success = True
		for nlp in self.nlp_pipelines:
			if nlp.name in nlp_to_run:
				self.status = "Running %s for %d trials (this may take a while)" % (nlp.name, num_nlp_trials)
				if self.catch_exceptions:
					try:
						nlp.run()
					except Exception, e:
						self.status = "Running %s failed: %s" % (nlp.name, str(e))
						success = False
						break
				else:
					nlp.run()
		
		# make sure we codified all criteria
		if success:
			for trial in trials:
				trial.codify_analyzables(self.nlp_pipelines, False)
			
			self.status = 'done'
		
		# run the callback
		if callback is not None:
			callback(success, trials)


	# -------------------------------------------------------------------------- NLP Pipelines
	def add_pipeline(self, nlp_pipeline):
		""" Add an NLP pipeline to the runner. """
		
		# set root directory
		nlp_pipeline.set_relative_root(self.run_dir)
		
		# add to stack
		if self.nlp_pipelines is None:
			self.nlp_pipelines = []
		self.nlp_pipelines.append(nlp_pipeline)
	
	def add_pipelines(self, nlp_pipelines):
		""" Add a bunch of NLP pipelines at once. """
		for nlp in nlp_pipelines:
			self.add_pipeline(nlp)
	
	
	# -------------------------------------------------------------------------- Status
	@property
	def name(self):
		if self._name is None:
			self._name = "find '%s'" % (self.condition if self.condition is not None else self.term)
		return self._name

	@property
	def status(self):
		if self._status is None:
			sqlite = SQLite.get(self.sqlite_db)
			if not sqlite:
				return None
			
			stat_query = "SELECT status FROM runs WHERE run_id = ?"
			res = sqlite.executeOne(stat_query, (self.run_id,))
			self._status = res[0] if res and len(res) > 0 else 'unknown status'
		
		return self._status

	@status.setter
	def status(self, status):
		logging.info("%s: %s" % (self.name, status))
		self._status = status
		
		sqlite = SQLite.get(self.sqlite_db)
		if sqlite:
			stat_query = "UPDATE runs SET status = ? WHERE run_id = ?"
			sqlite.executeUpdate(stat_query, (status, self.run_id))
			sqlite.commit()

	@property
	def done(self):
		return True if 'done' == self.status else False
	
	
	# -------------------------------------------------------------------------- Results
	def overview(self):
		if not self.done:
			raise Exception("Trial results are not yet available")
		
		sqlite = SQLite.get(self.sqlite_db)
		if sqlite is None:
			raise Exception("No SQLite handle, please set up properly")
		
		# collect intervention types and (drug) trial phases
		types = {}
		phases = {}
		qry = "SELECT types, phases FROM trials WHERE run_id = ?"
		for row in sqlite.execute(qry, (self.run_id,)):
			if row[0]:
				for tp in row[0].split('|'):
					types[tp] = types[tp] + 1 if tp in types else 1
			
			if row[1]:
				for ph in row[1].split('|'):
					phases[ph] = phases[ph] + 1 if ph in phases else 1
		
		return {
			'intervention_types': types,
			'drug_phases': phases
		}
	
	def trial_phases(self, filter_interventions=None):
		""" Return a dict with the number of trials per phase after filtering
		by intervention type. """
		if not self.done:
			raise Exception("Trial results are not yet available")
		
		sqlite = SQLite.get(self.sqlite_db)
		if sqlite is None:
			raise Exception("No SQLite handle, please set up properly")
		
		# collect (drug) trial phases
		phases = {}
		qry = "SELECT phases FROM trials WHERE run_id = ?"
		
		# filter by interventions
		if filter_interventions is not None:
			ored = []
			for inter in filter_interventions:
				ored.append('types LIKE "%%%s%%"' % inter)
			if len(ored) > 0:
				qry = qry + ' AND (' + ' OR '.join(ored) + ')'
		
		# execute query
		for row in sqlite.execute(qry, (self.run_id,)):
			if row[0]:
				for ph in row[0].split('|'):
					phases[ph] = phases[ph] + 1 if ph in phases else 1
		
		return phases
	
	def trials_json(self, filter_interventions=None, filter_phases=None):
		""" Returns an array of trial JSON for the matching trials, optionally
		filtered by intervention type and/or drug phases.
		"""
		if not self.done:
			raise Exception("Trial results are not yet available")
		
		sqlite = SQLite.get(self.sqlite_db)
		if sqlite is None:
			raise Exception("No SQLite handle, please set up properly")
		
		# look up trials. Currently cheaply filtering by string comparison
		qry = "SELECT nct FROM trials WHERE run_id = ?"
		tpls = [self.run_id]
		if filter_interventions is not None:
			ored = []
			for inter in filter_interventions:
				ored.append('types LIKE "%%%s%%"' % inter)
				# ored.append('instr(types, ?)')
				# tpls.append(inter)
			if len(ored) > 0:
				qry = qry + ' AND (' + ' OR '.join(ored) + ')'
		
		if filter_phases is not None:
			ored = []
			for phase in filter_phases:
				ored.append('phases LIKE "%%%s%%"' % phase)
				# ored.append('instr(phases, ?)')
				# tpls.append(phase)
			
			if len(ored) > 0:
				qry = qry + ' AND (' + ' OR '.join(ored) + ')'
		
		trials = []
		fields = ['keyword', 'phase', 'overall_contact']
		lat = float(self.reference_location[0]) if self.reference_location else 0
		lng = float(self.reference_location[1]) if self.reference_location else 0
		
		# retrieve ncts
		qry += ' ORDER BY distance ASC'
		for row in sqlite.execute(qry, tuple(tpls)):
			trial = Trial(row[0])
			trial.load()
			trial_dict = trial.json(fields)
			
			# add trial locations
			if lat and lng:
				closest = []
				for loc in trial.locations_closest_to(lat, lng, open_only=True):
					closest.append(loc[0].json())
				trial_dict['location'] = closest
			
			trials.append(trial_dict)
		
		# grab trial data in batch from db - PROBLEM: distance order is not preserved
		# for trial in Trial.retrieve(ncts):
		# 	trials.append(trial.json(fields))
		
		return trials


	def write_trial(self, sqlite, trial):
		""" Stores metadata about the given trial pertaining to the current run.
		"""
		if sqlite is None:
			raise Exception("No SQLite handle, please set up properly")
		
		# order by location
		distance = 99999
		if self.reference_location is not None:
			lat = float(self.reference_location[0])
			lng = float(self.reference_location[1])
			closest = trial.locations_closest_to(lat, lng, limit=1, open_only=True)
			
			if len(closest) > 0:
				distance = closest[0][1]
		
		nct_query = "INSERT INTO trials (run_id, nct, types, phases, distance) VALUES (?, ?, ?, ?, ?)"
		sqlite.executeInsert(nct_query, (
			self.run_id,
			trial.nct,
			'|'.join(trial.intervention_types),
			'|'.join(trial.trial_phases),
			distance
		))

	def write_trial_reason(self, nct, reason):
		""" ONLY TEMPORARY!!! """
		sqlite = SQLite.get(self.sqlite_db)
		if sqlite is None:
			raise Exception("No SQLite handle, please set up properly")
		
		nct_query = "UPDATE trials SET reason = ? WHERE nct = ?"
		sqlite.executeInsert(nct_query, (reason, nct))
		sqlite.commit()

	def get_ncts(self):
		""" Read the previously stored NCTs with their filtering reason (if any)
		and return them as a list of tuples. """
		sqlite = SQLite.get(self.sqlite_db)
		if sqlite is None:
			raise Exception("No SQLite handle, please set up properly")
		
		ncts = []
		nct_query = "SELECT nct, reason FROM trials WHERE run_id = ?"
		for res in sqlite.execute(nct_query, (self.run_id,)):
			ncts.append(res)
		
		return ncts


	# -------------------------------------------------------------------------- Run Directory
	def assure_run_directory(self):
		if self.run_dir is None:
			raise Exception("No run directory defined for runner %s" % self.name)
		
		# create our directory
		if not os.path.exists(self.run_dir):
			os.mkdir(self.run_dir)
		
		if not os.path.exists(self.run_dir):
			raise Exception("Failed to create run directory for runner %s" % self.name)
		
		# create our SQLite table
		sqlite = SQLite.get(self.sqlite_db)
		sqlite.execute('PRAGMA foreign_keys = ON')
		sqlite.create('runs', '''(
			run_id VARCHAR UNIQUE,
			date DATETIME DEFAULT CURRENT_TIMESTAMP,
			status VARCHAR
		)''')
		sqlite.create('trials', '''(
			run_id VARCHAR,
			nct VARCHAR,
			reason TEXT,
			types VARCHAR,
			phases VARCHAR,
			distance INT,
			UNIQUE (run_id, nct) ON CONFLICT REPLACE,
			FOREIGN KEY (run_id) REFERENCES runs (run_id) ON DELETE CASCADE
		)''')
		
		stat_query = "INSERT OR IGNORE INTO runs (run_id, status) VALUES (?, ?)"
		sqlite.executeInsert(stat_query, (self.run_id, 'initializing'))
		
		# clean older than 6 hours
		clean_qry = "DELETE FROM runs WHERE date < datetime('now', '-6 hours')"
		sqlite.execute(clean_qry, ())
		sqlite.commit()

