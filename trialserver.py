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
	
	def base_request(self, method, add_headers, url, data=None):
		headers = self.headers
		if add_headers is not None:
			headers.update(add_headers)
		
		return requests.Request(method, url, data=data, headers=headers)
	
	def trial_request(self, trial_id):
		if not self.base:
			raise Exception("The server's base URL is not defined")
		mth, api = self.search_endpoint.split(' ')
		if not mth or not api:
			raise Exception("Trial method and/or API endpoint is not defined")
		
		url = "{}{}".format(self.base, api.replace('{id}', trial_id))
		return self.base_request(mth, self.trial_headers, url)
	
	def search_request(self, finder, params, url=None):
		""" Returns a request that performs a search operation.
		
		:param finder: The `TrialFinder` instance asking for the request. You
			should ask the finder for additional information, like querying the
			`limit_*` properties.
		:param dict params: A dictionary with search parameters
		:param str url: You can override URL generation by providing it here.
			This is generally used to instantiate a request from a URL the
			service returned to get the next badge of results.
		:returns: A `requests` request instance
		"""
		if not self.base:
			raise Exception("The server's base URL is not defined")
		mth, api = self.search_endpoint.split(' ')
		if not mth or not api:
			raise Exception("Search method and/or API endpoint is not defined")
		
		if url is None:
			api_url = "{}{}".format(self.base, api)
			url, data = self.search_prepare_parts(api_url, finder, params)
		else:
			data = None
		
		return self.base_request(mth, self.search_headers, url, data)
	
	def search_prepare_parts(self, url, finder, params):
		""" Returns a tuple of URL and body data that should be used to
		construct the search request.
		
		By default appends all parameters as GET params and returns no body,
		`finder` is ignored.
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
		

