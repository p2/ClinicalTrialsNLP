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
import inspect
import subprocess

import xml.etree.ElementTree as ET

from nlp import NLPProcessing


class MetaMap (NLPProcessing):
	""" Aggregate handling tasks specifically for MetaMap. """
	
	def __init__(self):
		super(MetaMap, self).__init__()
		self.name = 'metamap'
		self.bin = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
	
	
	@property
	def _in_dir(self):
		return os.path.join(self.root, 'metamap_input')
	
	@property
	def _out_dir(self):
		return os.path.join(self.root, 'metamap_output')
	
	def _create_directories_if_needed(self):
		in_dir = self._in_dir
		out_dir = self._out_dir
		if not os.path.exists(in_dir):
			os.mkdir(in_dir)
		if not os.path.exists(out_dir):
			os.mkdir(out_dir)
	
	def _run(self):
		try:
			subprocess.call(['%s/metamap/run.sh' % self.bin, self.root], stderr=subprocess.STDOUT)
		except subprocess.CalledProcessError, e:
			raise Exception(e.output)
	
	
	def _write_input(self, text, filename):
		if text is None or len(text) < 1 \
			or filename is None:
			return False
		
		in_dir = self._in_dir
		if not os.path.exists(in_dir):
			logging.error("The input directory for MetaMap at %s does not exist" % in_dir)
			return False
		
		infile = os.path.join(in_dir, filename)
		if os.path.exists(infile):
			return False
		
		# write it
		with codecs.open(infile, 'w', 'ascii') as handle:
			handle.write(text.encode('ascii', 'ignore'))
		
		return True
	
	
	def _parse_output(self, filename, **kwargs):
		""" Parse MetaMap XML output.
		We currently only retrieve the mappings, not the candidates (there is
		the flag "with_candidates" to turn it on). You can pass 'filter_sources'
		to only report codes of SNOMEDCT and MTH sources.
		"""
		if filename is None:
			return None
		
		# is there output?
		root = self.root if self.root is not None else '.'
		out_dir = self._out_dir
		if not os.path.exists(out_dir):
			logging.error("The output directory for MetaMap at %s does not exist" % out_dir)
			return None
		
		outfile = os.path.join(out_dir, filename)
		if not os.path.exists(outfile):
			return None
		
		with_candidates = False
		filter_sources = 'filter_sources' in kwargs
		
		text_phrases = []
		cuis = set()
		
		# parse XMI file
		try:
			root = ET.parse(outfile).getroot()
		except Exception as e:
			logging.error("Failed to parse MetaMap output file %s:  %s" % (outfile, e))
			return None
		
		# find utterances
		utterances = root.findall('./MMO/Utterances/Utterance')
		for utterance in utterances:
			full_text = utterance.find('./UttText').text
			text_phrases.append(full_text)
			
			# find phrases
			phrases = utterance.findall('./Phrases/Phrase')
			for phrase in phrases:
				phrase_start = int(phrase.find('./PhraseStartPos').text)
				phrase_length = int(phrase.find('./PhraseLength').text)
				
				candidates = phrase.findall('./Mappings/Mapping/MappingCandidates/Candidate')
				
				if with_candidates:
					candidates.extend(phrase.findall('./Candidates/Candidate'))
				
				# pull out codes from all candidate nodes
				for candidate in candidates:
					
					# check sources if "filter_sources" is on
					use = True
					if filter_sources:
						use = False
						srcNodes = candidate.findall('./Sources/Source')
						
						# only use if the code is from SNOMED or Metathesaurus
						usable = ['SNOMEDCT', 'MTH']
						for src in srcNodes:
							if src.text in usable:
								use = True
								break
					
					# get CUI
					cui = None
					if use:
						cuiNode = candidate.find('CandidateCUI')
						cui = cuiNode.text if cuiNode is not None else '???'
						
						# check negation
						negNode = candidate.find('Negated')
						if 1 == int(negNode.text):
							cui = '-%s' % cui
					
					# add phrase position to CUI
					if cui is not None:
						cui = '%s@%d+%d' % (cui, phrase_start, phrase_length)
						cuis.add(cui)
		
		# clean up if instructed to do so
		if self.cleanup:
			os.remove(outfile)
			
			in_dir = self._in_dir
			infile = os.path.join(in_dir, filename)
			if os.path.exists(infile):
				os.remove(infile)
		
		# create and return a dictionary
		ret = {}
		if len(text_phrases) > 0:
			ret['text'] = ''.join(text_phrases)
		if len(cuis) > 0:
			ret['cui'] = list(cuis)
		
		return ret


# we can execute this file to do some testing
if '__main__' == __name__:
	print "-->  Testing MetaMap"
	
	testtext = "History of clincally significant hypogammaglobulinemia, common variable immunodeficiency, or humeral immunodeficiency."
	testfile = 'test.txt'
	
	# instantiate and prepare
	run_dir = os.path.join(os.path.dirname(__file__), 'metamap-test')
	my_mm = MetaMap()
	my_mm.root = run_dir
	
	# create test input
	if not my_mm.write_input(testtext, testfile):
		print "xx>  Failed to write test input to file"
	
	# run
	try:
		my_mm.run()
	except Exception as e:
		print "xx>  Failed: %s" % e
	
	# parse output
	ret = my_mm.parse_output(testfile)
	if 'cui' not in ret \
		or ret['cui'] is None \
		or 0 == len(ret['cui']):
		print "xx>  Failed to extract CUI from sample text"
	else:
		print "-->  Found %d CUIs in test text" % len(ret['cui'])
	
	# clean up
	if my_mm.cleanup:
		os.rmdir(os.path.join(run_dir, 'metamap_input'))
		os.rmdir(os.path.join(run_dir, 'metamap_output'))
		os.rmdir(run_dir)
	
	print "-->  Done"
