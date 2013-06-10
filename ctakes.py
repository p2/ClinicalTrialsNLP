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


class cTAKES (NLPProcessing):
	""" Aggregate handling tasks specifically for cTAKES. """
	
	def __init__(self, settings):
		super(cTAKES, self).__init__(settings)
		self.name = 'ctakes'
	
	
	def write_input(self, text, filename):
		if text is None or len(text) < 1:
			return False
		
		if filename is None:
			return False
		
		in_dir = os.path.join(self.root if self.root is not None else '.', 'ctakes_input')
		if not os.path.exists(in_dir):
			logging.error("The input directory for cTAKES at %s does not exist" % in_dir)
			return False
		
		infile = os.path.join(in_dir, filename)
		if os.path.exists(infile):
			return False
		
		# write it
		with codecs.open(infile, 'w', 'utf-8') as handle:
			handle.write(list_to_sentences(text))
		
		return True
	
	
	def parse_output(self, filename, **kwargs):
		""" Parse cTAKES XML output. """
		
		if filename is None:
			return None
		
		# is there cTAKES output?
		root = self.root if self.root is not None else '.'
		out_dir = os.path.join(root, 'ctakes_output')
		if not os.path.exists(out_dir):
			logging.error("The output directory for cTAKES at %s does not exist" % out_dir)
			return None
		
		outfile = os.path.join(out_dir, "%s.xmi" % filename)
		if not os.path.exists(outfile):
			# do not log here and silently fail
			return None
		
		snomeds = []
		cuis = []
		rxnorms = []
		
		# parse XMI file
		root = parse(outfile).documentElement
		
		# pluck apart nodes that carry codified data ("refsem" namespace)
		code_nodes = root.getElementsByTagNameNS('http:///org/apache/ctakes/typesystem/type/refsem.ecore', '*')
		if len(code_nodes) > 0:
			for node in code_nodes:
				#print node.toprettyxml()
				
				if 'codingScheme' in node.attributes.keys() \
					and 'code' in node.attributes.keys():
					
					# extract SNOMED code
					if 'SNOMED' == node.attributes['codingScheme'].value:
						snomeds.append(node.attributes['code'].value)
					
					# extract RXNORM code
					elif 'RXNORM' == node.attributes['codingScheme'].value:
						rxnorms.append(node.attributes['code'].value)
				
				# extract UMLS CUI
				if 'cui' in node.attributes.keys():
					cuis.append(node.attributes['cui'].value)
			
			# make lists unique
			snomeds = list(set(snomeds))
			cuis = list(set(cuis))
			rxnorms = list(set(rxnorms))
		
		# clean up if instructed to do so
		if self.cleanup:
			os.remove(outfile)
			
			in_dir = os.path.join(root, 'ctakes_input')
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

