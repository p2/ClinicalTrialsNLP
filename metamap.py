#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#	Handling cTAKES
#
#	2013-05-14	Created by Pascal Pfiffner
#

import os
import logging
import codecs

from xml.dom.minidom import parse

from nlp import NLPProcessing, list_to_sentences


class MetaMap (NLPProcessing):
	""" Aggregate handling tasks specifically for MetaMap. """
	
	def __init__(self, settings):
		super(MetaMap, self).__init__(settings)
		self.name = 'metamap'
	
	
	def write_input(self, text, filename):
		if text is None or len(text) < 1:
			return False
		
		if filename is None:
			return False
		
		in_dir = os.path.join(self.root if self.root is not None else '.', 'metamap_input')
		if not os.path.exists(in_dir):
			logging.error("The input directory for MetaMap at %s does not exist" % in_dir)
			return False
		
		infile = os.path.join(in_dir, filename)
		if os.path.exists(infile):
			return False
		
		# write it
		with codecs.open(infile, 'w', 'utf-8') as handle:
			handle.write(list_to_sentences(text))
		
		return True
	
	
	def parse_output(self, filename, **kwargs):
		""" Parse MetaMap XML output.
		We currently only retrieve the mappings, not the candidates (there is
		the flag "with_candidates" to turn it on). You can pass 'filter_sources'
		to only report codes of SNOMEDCT and MTH sources.
		"""
		if filename is None:
			return None
		
		# is there output?
		root = self.root if self.root is not None else '.'
		out_dir = os.path.join(root, 'metamap_output')
		if not os.path.exists(out_dir):
			logging.error("The output directory for MetaMap at %s does not exist" % out_dir)
			return None
		
		outfile = os.path.join(out_dir, filename)
		if not os.path.exists(outfile):
			return None
		
		with_candidates = False
		filter_sources = 'filter_sources' in kwargs
		
		# parse XMI file
		try:
			root = parse(outfile).documentElement
		except Exception, e:
			logging.error("Failed to parse MetaMap output file %s:  %s" % (outfile, e))
			return None
		
		# find mappings
		candidates = []
		try:
			foo = root.getElementsByTagName('MMO')[0].getElementsByTagName('Utterances')[0]
			utterances = foo.getElementsByTagName('Utterance')
			for utter in utterances:
				phrases = utter.getElementsByTagName('Phrases')[0].getElementsByTagName('Phrase')
				for phrase in phrases:
					if with_candidates:
						candidates.extend(phrase.getElementsByTagName('Candidates')[0].getElementsByTagName('Candidate'))
					
					# get the mapping candidate candidates
					mappings = phrase.getElementsByTagName('Mappings')[0].getElementsByTagName('Mapping')
					for mapping in mappings:
						for cand in mapping.getElementsByTagName('MappingCandidates'):
							candidates.extend(cand.getElementsByTagName('Candidate'))
			
			# think about parsing negations in "Negations"
			
		except Exception, e:
			logging.warning("Exception while parsing MetaMap output: %s" % e)
			pass
		
		# pull out codes from all candidate nodes
		snomeds = []
		cuis = []
		rxnorms = []
		for candidate in candidates:
			# matchNodes = candidate.getElementsByTagName('CandidateMatched')
			# if 1 != len(matchNodes):
			# 	logginge.error("Only expecting one CandidateMatched node, but got %d" % len(matchNodes))
			# 	continue
			# match = matchNodes[0]
			# if 1 != len(match.childNodes):
			# 	logginge.error("Only expecting one child node in our CandidateMatched node, but got %d" % len(match.childNodes))
			# 	continue
			
			# check sources if "filter_sources" is on
			use = True
			if filter_sources:
				use = False
				srcParent = candidate.getElementsByTagName('Sources')
				
				if 1 == len(srcParent):
					srcNodes = srcParent[0].getElementsByTagName('Source')
					
					# only use if the code is from SNOMED or Metathesaurus
					usable = ['SNOMEDCT', 'MTH']
					for src in srcNodes:
						if src.childNodes[0].nodeValue in usable:
							use = True
							break
			
			# get CUI
			cui = None
			if use:
				cuiNodes = candidate.getElementsByTagName('CandidateCUI')
				if 1 == len(cuiNodes):
					cuiN = cuiNodes[0]
					cui = cuiN.childNodes[0].nodeValue
			
			if cui is not None:
				cuis.append(cui)
		
		# clean up if instructed to do so
		if self.cleanup:
			os.remove(outfile)
			
			in_dir = os.path.join(root, 'metamap_input')
			infile = os.path.join(in_dir, filename)
			if os.path.exists(infile):
				os.remove(infile)
		
		# create and return a dictionary
		ret = {}
		if len(snomeds) > 0:
			ret['snomed'] = snomeds
		if len(cuis) > 0:
			ret['cui'] = cuis
		if len(rxnorms) > 0:
			ret['rxnorm'] = rxnorms
		
		return ret

