#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import requests


class TrialFinder(object):
	""" Find trials on a server.
	"""
	
	def __init__(self, server):
		assert(server)
		self.server = server
		self.session = None
		self.limit_only_recruiting = True
		self.limit_countries = None
		self.search_current = None
		self.search_more = None
		self.search_meta = None
	
	def find(self, params):
		""" Find trials with the given parameters.
		"""
		req = self.server.search_request(self, params)
		trials = self._find(req)
		total = self.search_meta.get('total') or 0
		if total <= 200:
			while self.hasMore():
				trials.extend(self.more())
		
		return trials
	
	def _find(self, req):
		""" Execute the given request in the search context.
		
		:returns: A list of Trial instances; may be empty but never None
		"""
		self.search_current = req
		res = self.request_json(req)
		trials, meta, more = self.server.search_process_response(res)
		self.search_meta = meta
		if more is not None:
			self.search_more = self.server.search_request(self, None, more)
		else:
			self.search_more = None
		
		return trials or []
	
	def hasMore(self):
		return self.search_more is not None
	
	def more(self):
		""" Performs the `search_more` request if there is one.
		"""
		if self.search_more is not None:
			return self._find(self.search_more)
		return []
	
	def request_json(self, request):
		if self.session is None:
			self.session = requests.Session()
		prepped = self.session.prepare_request(request)
		res = self.session.send(prepped)
		res.raise_for_status()
		
		return res.json()
