#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests


class TrialServer(object):
	""" A server serving clinical trials.
	
	Responses are expected to be JSON for now.
	"""
	
	def __init__(self, base, api_key=None):
		self.base = base
		self.api_key = api_key
		self.headers = {}				# header dictionary to send with requests
		self.trial_endpoint = 'GET /trials/{id}'
		self.trial_headers = {}			# in addition to `headers`
		self.search_endpoint = 'GET /trials/search'
		self.search_headers = {}		# in addition to `headers`
	
	def trial_request(self, trial_id):
		if not self.base:
			raise Exception("The server's base URL is not defined")
		mth, api = self.search_endpoint.split(' ')
		if not mth or not api:
			raise Exception("Trial method and/or API endpoint is not defined")
		
		headers = self.headers
		headers.update(self.trial_headers)
		
		url = "{}{}".format(self.base, api.replace('{id}', trial_id))
		
		return requests.Request(mth, url, data=None, headers=headers)
	
	def search_request(self, params, override_uri=None):
		if not self.base:
			raise Exception("The server's base URL is not defined")
		mth, api = self.search_endpoint.split(' ')
		if not mth or not api:
			raise Exception("Search method and/or API endpoint is not defined")
		
		headers = self.headers
		headers.update(self.search_headers)
		
		data = None
		if override_uri is not None:
			url = override_uri
		else:
			api_url = "{}{}".format(self.base, api)
			url, data = self.search_prepare_parts(api_url, params)
		
		return requests.Request(mth, url, data=data, headers=headers)
	
	def search_prepare_parts(self, url, params):
		""" Returns a tuple of URL and body data that should be used to
		construct the search request.
		
		By default appends all parameters as GET params and returns no body.
		"""
		par = []
		for key, val in params.items():
			par.append("{}={}".format(key, val.replace(' ', '+')))
		
		url = "{}?{}".format(url, '&'.join(par))
		return url, None
	
	def search_process_response(self, response):
		""" Takes response data and returns a list of Trial instances, a
		meta dictionary and the URL to retrieve to get more results (if
		applicable).
		
		By default assumes a 'results' and 'meta' dictionary.
		"""
		trials = []
		meta = response.get('meta')
		results = response.get('results') or []
		for result in results:
			trial = Trial(result.get('id'), result)
			trials.append(trial)
		
		return trials, meta, None
		

