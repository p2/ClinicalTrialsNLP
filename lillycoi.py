#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#	Talk to ClinicalTrials.gov via Lilly's API
#	http://portal.lillycoi.com/api-reference-guide/
#
#	2012-12-12	Created by Pascal Pfiffner
#

import httplib2
import json
import logging

from study import Study


class LillyCOI (object):
	""" A class to use Lilly's bridge-API for ClinicalTrials.gov.
	
	See the reference guide here:
	http://portal.lillycoi.com/api-reference-guide/
	
	ClinicalTrials.gov API "documentation":
	http://clinicaltrials.gov/ct2/info/linking
	"""
	
	baseURL = 'http://api.lillycoi.com/v1'
	
	
	def __init__(self):
		self.http = httplib2.Http()
		self.previousPageURI = None
		self.nextPageURI = None
		self.resultCount = 0
		self.totalCount = 0
	

	# -------------------------------------------------------------------------- Searching for Trials
	def search_for_condition(self, condition, recruiting=None, fields=[]):
		""" Search trials matching a given condition.
		
		condition -- The condition to search for
		recruiting -- None to not limit to recruiting status, otherwise True or
			False
		fields -- A list of fields to return. Defaults to id and title
		"""
		
		if condition is None or len(condition) < 1:
			raise Exception('You must provide a condition to search for')
		
		cond = condition.replace(' ', '+')
		if recruiting is not None:
			recr = 'open' if recruiting is True else 'closed'
			query = 'recr:%s,cond:%s' % (recr, cond)
		else:
			query = 'cond:%s' % cond
		
		return self.search_for(query, fields)
	
	def search_for_term(self, term, recruiting=None, fields=[]):
		""" Search trials with a generic search term.
		
		term -- The term to search for
		recruiting -- None to not limit to recruiting status, otherwise True or
			False
		fields -- A list of fields to return. Defaults to id and title
		"""
		
		if term is None or len(term) < 1:
			raise Exception('You must provide a term to search for')
		
		trm = term.replace(' ', '-')
		if recruiting is not None:
			recr = 'open' if recruiting is True else 'closed'
			query = 'recr:%s,term:%s' % (recr, trm)
		else:
			query = 'term:%s' % trm
		
		return self.search_for(query, fields)
	
	
	def search_for(self, query, fields=[]):
		""" Performs the search for the given (already prepared) query.
		If no fields to retrieve are given we just get the number of studies.
		"""
		if query is None:
			raise Exception("You must provide a query parameter")
		
		# compose the URL
		flds = 'id'
		limit = 50
		loop = True
		if fields is None or len(fields) < 1:
			limit = 1
			loop = False
		else:
			flds = ','.join(fields)
		
		params = 'fields=%s&limit=%d&query=%s' % (flds, limit, query)
		
		# loop page after page
		results = self.get('trials/search.json', params)
		if loop:
			while self.nextPageURI is not None:
				myNext = self.nextPageURI
				self.nextPageURI = None					# reset here in case of error
				results.extend(self._get(myNext))		# will set nextPageURI on success
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
		
		url = '%s/%s' % (LillyCOI.baseURL, method)
		if parameters is not None:
			url = '%s?%s' % (url, parameters)
		return self._get(url)


	# the base GET grabber
	def _get(self, url):
		logging.debug('-->  GET: %s' % url)
		headers = {}
		
		# fire it off
		response, content = self.http.request(url, 'GET', headers=headers)
		
		# decode JSON
		data = {}
		try:
			data = json.loads(content)
		except Exception, e:
			logging.error("-----\n%s\n-----\n%s\n-----" % (e, content))
			return []
		
		self.previousPageURI = data.get('previousPageURI')
		self.nextPageURI = data.get('nextPageURI')
		self.resultCount = data.get('resultCount')
		self.totalCount = data.get('totalCount')
		
		# instantiate study objects
		studies = []
		for s in data.get('results', []):
			study = Study()
			study.from_dict(s)
			studies.append(study)
		
		return studies

