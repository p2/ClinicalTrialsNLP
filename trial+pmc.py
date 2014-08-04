#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
#	Extension of the `Trial` class by a few PubMedCentral related methods.
#
#	2012-12-13	Created by Pascal Pfiffner
#	2014-08-04	Extracted from trial.py

import trial



def run_pmc(self, run_dir):
	""" Finds, downloads, extracts and parses PMC-indexed publications for
	the trial. """
	self.find_pmc_packages()
	self.download_pmc_packages(run_dir)
	self.parse_pmc_packages(run_dir)

trial.Trial.run_pmc = run_pmc


def find_pmc_packages(self):
	""" Determine whether there was a PMC-indexed publication for the trial.
	"""
	if self.nct is None:
		logging.warning("Need an NCT before trying to find publications")
		return
	
	# find paper details
	self._papers = Paper.find_by_nct(self.nct)
	for paper in self._papers:
		paper.fetch_pmc_ids()

trial.Trial.find_pmc_packages = find_pmc_packages


def download_pmc_packages(self, run_dir):
	""" Downloads the PubMed Central packages for our papers. """
	
	if self._papers is not None:
		for paper in self._papers:
			paper.download_pmc_packages(run_dir)

trial.Trial.download_pmc_packages = download_pmc_packages


def parse_pmc_packages(self, run_dir):
	""" Looks for downloaded packages in the given run directory and
	extracts the paper text from the XML in the .nxml file.
	"""
	if self._papers is None:
		return
	
	import os.path
	if not os.path.exists(run_dir):
		raise Exception("The run directory %s doesn't exist" % run_dir)
	
	import codecs
	for paper in self._papers:
		paper.parse_pmc_packages(run_dir, None)
		
		# also dump CT criteria if the paper has methods
		if paper.has_methods:
			plaintextpath = os.path.join(ct_in_dir, "%s-%s-CT.txt" % (self.nct, paper.pmid))
			with codecs.open(plaintextpath, 'w', 'utf-8') as handle:
				handle.write(self.eligibility.formatted())

trial.Trial.parse_pmc_packages = parse_pmc_packages
