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
	
	def api_request(self, method, add_headers, path, data=None, override_url=None):
		if not self.base:
			raise Exception("The server's base URL is not defined")
		
		url = override_url
		if url is None:
			url = "{}{}".format(self.base, path)
		
		return self.base_request(method, add_headers, url, data)
	
	def trial_request(self, trial_id):
		mth, api = self.search_endpoint.split(' ')
		if not mth or not api:
			raise Exception("Trial method and/or API endpoint is not defined")
		
		return self.api_request(mth, self.trial_headers, api.replace('{id}', trial_id))
	
	def search_request(self, params, override_url=None):
		""" Returns a request that performs a search operation.
		
		:param dict params: A dictionary with search parameters and limitations.
			Special limitations to support are:
			- "countries": A list of country names to limit search to
			- "recruiting": A bool flag whether only recruiting trials should
				be reported
		:param str override_url: You can override URL generation by providing it
			here. This is generally used to instantiate a request from a URL the
			service returned to get the next badge of results.
		:returns: A `requests` request instance
		"""
		mth, api = self.search_endpoint.split(' ')
		if not mth or not api:
			raise Exception("Search method and/or API endpoint is not defined")
		
		path = None
		data = None
		if override_url is None:
			path, data = self.search_prepare_parts(api, params)
		
		return self.api_request(mth, self.search_headers, path, data, override_url)
	
	def search_prepare_parts(self, path, params):
		""" Returns a tuple of path and body data that should be used to
		construct the search request.
		
		By default appends all parameters (except "countries") as GET params
		and returns no body.
		"""
		prms = params
		if 'countries' in prms:
			del prms['countries']		# subclasses must build proper support
		
		par = []
		for key, val in prms.items():
			par.append("{}={}".format(key, val.replace(' ', '+')))
		
		path = "{}?{}".format(path, '&'.join(par))
		return path, None
	
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
		

