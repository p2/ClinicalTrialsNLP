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

from xml.dom.minidom import parse
from subprocess import call

from nlp import NLPProcessing, list_to_sentences


class cTAKES (NLPProcessing):
	""" Aggregate handling tasks specifically for cTAKES. """
	
	def __init__(self, settings):
		super(cTAKES, self).__init__(settings)
		self.name = 'ctakes'
		self.bin = os.path.dirname(os.path.abspath('%s/../' % inspect.getfile(inspect.currentframe())))
	
	
	@property
	def _in_dir(self):
		return os.path.join(self.root, 'ctakes_input')
	
	@property
	def _out_dir(self):
		return os.path.join(self.root, 'ctakes_output')
	
	def _create_directories_if_needed(self):
		in_dir = self._in_dir
		out_dir = self._out_dir
		if not os.path.exists(in_dir):
			os.mkdir(in_dir)
		if not os.path.exists(out_dir):
			os.mkdir(out_dir)
	
	def _run(self):
		if call(['%s/ctakes/run.sh' % self.bin, self.root]) > 0:
			raise Exception('Error running cTakes')
	
	def write_input(self, text, filename):
		if text is None \
			or len(text) < 1 \
			or filename is None:
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
		
		# get all "textsem:EntityMention" which store negation information
		neg_ids = []
		for node in root.getElementsByTagName('textsem:EntityMention'):
			polarity = node.attributes.get('polarity')
			if polarity is not None and int(polarity.value) < 0:
				ids = node.attributes.get('ontologyConceptArr')
				if ids is not None and ids.value:
					neg_ids.extend([int(i) for i in ids.value.split()])
		
		# pluck apart nodes that carry codified data ("refsem" namespace)
		code_nodes = root.getElementsByTagNameNS('http:///org/apache/ctakes/typesystem/type/refsem.ecore', '*')
		if len(code_nodes) > 0:
			for node in code_nodes:
				#print node.toprettyxml()
				
				# check if this node is negated
				is_neg = False
				node_id_attr = node.attributes.get('xmi:id')
				if node_id_attr is not None:
					is_neg = int(node_id_attr.value) in neg_ids
				
				# extract SNOMED and RxNORM
				if 'codingScheme' in node.attributes.keys() \
					and 'code' in node.attributes.keys():
					code = node.attributes['code'].value
					if is_neg:
						code = "-%s" % code
					
					# extract SNOMED code
					if 'SNOMED' == node.attributes['codingScheme'].value:
						snomeds.append(code)
					
					# extract RXNORM code
					elif 'RXNORM' == node.attributes['codingScheme'].value:
						rxnorms.append(code)
				
				# extract UMLS CUI
				if 'cui' in node.attributes.keys():
					code = node.attributes['cui'].value
					if is_neg:
						code = "-%s" % code
					cuis.append(code)
			
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
		
		# create and return a dictionary (don't filter empty lists)
		ret = {
			'snomed': snomeds,
			'cui': cuis,
			'rxnorm': rxnorms
		}

		return ret


# we can execute this file to do some testing
if '__main__' == __name__:
	run_dir = os.path.join(os.path.dirname(__file__), 'ctakes-test')
	myct = cTAKES({'root': run_dir, 'cleanup': True})
	myct.prepare()
	
	# create a test input file
	with open(os.path.join(myct.root, 'ctakes_input/test.txt'), 'w') as handle:
		handle.write("History of clincally significant hypogammaglobulinemia, common variable immunodeficiency, or humeral immunodeficientncy")
	
	# run
	print "-->  Starting"
	try:
		myct.run()
	except Exception, e:
		print "xx>  Failed: %s" % e
	
	# TODO: parse output
	
	print "-->  Done"
