#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#	Talk to ClinicalTrials.gov via Lilly's API
#	http://portal.lillycoi.com/api-reference-guide/
#
#	2012-12-12	Created by Pascal Pfiffner
#

import json
import logging
import requests
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.WARNING)

from trial import Trial


class LillyCOI (object):
	""" A class to use Lilly's bridge-API for ClinicalTrials.gov.
	
	See the reference guide here:
	http://portal.lillycoi.com/api-reference-guide/
	
	ClinicalTrials.gov API "documentation":
	http://clinicaltrials.gov/ct2/info/linking
	"""
	
	baseURL = 'http://api.lillycoi.com/v1'
	
	
	def __init__(self):
		self.previousPageURI = None
		self.nextPageURI = None
		self.totalCount = 0
	

	# -------------------------------------------------------------------------- Searching for Trials
	def get_trial(self, nct):
		""" Retrieve one single trial. """
		
		assert nct is not None
		method = "trials/%s.json" % nct.strip()
		trials = self.get(method)
		if len(trials) > 1:
			raise Exception("Got more than one trial for identifier %s" % nct)
		if len(trials) > 0:
			return trials[0]
		return None
	
	
	def search_for_condition(self, condition, recruiting=None, fields=None, progress_func=None):
		""" Search trials matching a given condition.
		
		condition -- The condition to search for
		recruiting -- None to not limit to recruiting status, otherwise True or
			False
		fields -- A list of fields to return.
		progress_func -- A function that may (!!!) be called with the receiver's
			instance as the first and the progress ratio as second argument
		"""
		
		if condition is None or len(condition) < 1:
			raise Exception('You must provide a condition to search for')
		
		cond = condition.replace(' ', '+')
		if recruiting is not None:
			recr = 'open' if recruiting is True else 'closed'
			query = 'recr:%s,cond:%s' % (recr, cond)
		else:
			query = 'cond:%s' % cond
		
		return self.search_for(query, fields, progress_func)
	
	def search_for_term(self, term, recruiting=None, fields=None, progress_func=None):
		""" Search trials with a generic search term.
		
		term -- The term to search for
		recruiting -- None to not limit to recruiting status, otherwise True or
			False
		progress_func -- A function that may (!!!) be called with the receiver's
			instance as the first and the progress ratio as second argument
		"""
		
		if term is None or len(term) < 1:
			raise Exception('You must provide a term to search for')
		
		trm = term.replace(' ', '+')
		if recruiting is not None:
			recr = 'open' if recruiting is True else 'closed'
			query = 'recr:%s,term:%s' % (recr, trm)
		else:
			query = 'term:%s' % trm
		
		return self.search_for(query, fields, progress_func)
	
	
	def search_for(self, query, fields=None, progress_func=None):
		""" Performs the search for the given (already prepared) query.
		If fields is None, we just get the number of trials. If it is an array
		(even if empty) we ensure that we at least get the NCT-number (id),
		acronym, brief_title and official_title.
		"""
		if query is None:
			raise Exception("You must provide a query parameter")
		
		# handle fields: if None do nothing, otherwise make sure we have basic fields
		if fields is not None:
			for item in ['id', 'acronym', 'brief_title', 'official_title']:
				if item not in fields:
					fields.append(item)
		
		# compose the URL
		flds = 'id'
		limit = 50
		loop = True
		if fields is None or 0 == len(fields):
			limit = 1
			loop = False
		else:
			flds = ','.join(fields)
		
		params = 'fields=%s&limit=%d&query=%s' % (flds, limit, query)
		
		# loop page after page
		results = self.get('trials/search.json', params)
		if loop:
			i = 0
			while self.nextPageURI is not None:
				myNext = self.nextPageURI
				self.nextPageURI = None					# reset here in case of error
				if progress_func is not None and self.totalCount is not None:
					progress_func(self, float((i + 1) * limit) / self.totalCount)
				results.extend(self._get(myNext))		# will set nextPageURI on success
				i += 1
		else:
			self.nextPageURI = None
			if self.totalCount is not None:
				results = [None for foo in xrange(self.totalCount)]
		
		return results
	
	
	def num_for_condition(self, condition, recruiting=True):
		""" Count the number of results you would get for the given search. """
		if condition is None or len(condition) < 1:
			raise Exception('You must provide a condition to search for')
		
		cond = condition.replace(' ', '-')
		recr = 'open' if recruiting is True else 'closed'
		params = 'fields=id&limit=1&query=recr:%s,cond:%s' % (recr, cond)
		return self.get('trials/search.json', params)
	
	
	# -------------------------------------------------------------------------- Network
	def get(self, method, parameters=None):
		"""Performs a GET request against Lilly's base URL and decodes the JSON
		to a dictionary/array representation.
		"""
		
		url = '%s/%s' % (self.__class__.baseURL, method)
		if parameters is not None:
			url = '%s?%s' % (url, parameters)
		return self._get(url)


	# the base GET grabber
	def _get(self, url):
		logging.debug('-->  GET: %s' % url)
		
		# fire it off
		res = requests.get(url)
		if not res.ok:
			logging.error("xx>  %s when getting %s: %s" % (res.status_code, url, res.error))
			return []
		
		# decode JSON
		data = {}
		try:
			data = json.loads(res.content)
		except Exception, e:
			logging.error("-----\n%s\n-----\n%s\n-----" % (e, res.content))
			return []
		
		self.previousPageURI = data.get('previousPageURI')
		self.nextPageURI = data.get('nextPageURI')
		if self.nextPageURI:
			self.nextPageURI = self.nextPageURI.replace(' ', '+')	# some queries come back with a space!
		self.totalCount = int(data.get('totalCount', 1))
		
		# instantiate Trial objects
		trials = []
		for tr in data.get('results', []):
			trial = Trial()
			trial.update_from_lilly(tr)
			trials.append(trial)
		
		return trials

