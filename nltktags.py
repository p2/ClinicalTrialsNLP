#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#	Handling NLTK to generate tags
#
#	2013-10-25	Created by Pascal Pfiffner
#

import os
import logging
import codecs
import inspect
import nltk
import operator

from nlp import NLPProcessing, list_to_sentences


class NLTKTags (NLPProcessing):
	""" Aggregate handling tasks specifically for NLTK. """
	
	def __init__(self, settings):
		super(NLTKTags, self).__init__(settings)
		self.name = 'nltk-tags'
	
	
	@property
	def _in_dir(self):
		return os.path.join(self.root, 'nltk-tags-in')
	
	@property
	def _out_dir(self):
		return os.path.join(self.root, 'nltk-tags-out')
	
	def _create_directories_if_needed(self):
		in_dir = self._in_dir
		out_dir = self._out_dir
		if not os.path.exists(in_dir):
			os.mkdir(in_dir)
		if not os.path.exists(out_dir):
			os.mkdir(out_dir)
	
	def _run(self):
		in_dir = self._in_dir
		out_dir = self._out_dir
		if not os.path.exists(in_dir) or not os.path.exists(out_dir):
			return
		
		# init our simple noun-phrase chunker
		grammar = r"""
		    NBAR:
		        {<NN.*|JJ>*<NN.*>}  # Nouns and Adjectives, terminated with Nouns
		        
		    NP:
		        {<NBAR>}
		        {<NBAR><IN><NBAR>}  # Above, connected with in/of/etc...
		"""
		chunker = nltk.RegexpParser(grammar)
		
		filelist = os.listdir(in_dir)
		tag_count = {}
		i = 0
		for f in filelist:
			i = i + 1
			logging.debug("  Reading file %d of %d" % (i, len(filelist)))
			with codecs.open(os.path.join(in_dir, f), 'r', 'utf-8') as handle:
				text = handle.read()
				
				# use NLTK to chunk the text
				# import pdb; pdb.set_trace()
				chunks = []
				sentences = nltk.sent_tokenize(text)
				if sentences and len(sentences) > 0:
					for sentence in sentences:
						tokens = nltk.word_tokenize(sentence)
						tagged = nltk.pos_tag(tokens)
						tree = chunker.parse(tagged)
						
						# get noun phrases
						np = []
						for st in _nltk_find_leaves(tree, 'NP'):
							leaves = st.leaves()
							if len(leaves) > 0:
								tag = ' '.join([noun[0] for noun in leaves]).lower()
								np.append(tag)
								
								# count tags
								if tag in tag_count:
									tag_count[tag] = tag_count[tag] + 1
								else:
									tag_count[tag] = 1
						
						if len(np) > 0:
							chunks.extend(np)
				
				# write to outfile
				if len(chunks) > 0:
					outfile = os.path.join(out_dir, f)
					with codecs.open(outfile, 'w', 'utf-8') as w_handle:
						for chunk in chunks:
							w_handle.write("%s\n" % unicode(chunk))
		
		# tag count
		if len(tag_count) > 0:
			with codecs.open(os.path.join(out_dir, 'tags.txt'), 'w', 'utf-8') as handle:
				for tag in sorted(tag_count.iteritems(), key=operator.itemgetter(1), reverse=True):
					handle.write("%s: %d\n" % (tag[0], int(tag[1])))
	
	
	def write_input(self, text, filename):
		if text is None \
			or len(text) < 1 \
			or filename is None:
			return False
		
		in_dir = self._in_dir
		if not os.path.exists(in_dir):
			logging.error("The input directory for %s at %s does not exist" % (self.name, in_dir))
			return False
		
		infile = os.path.join(in_dir, filename)
		if os.path.exists(infile):
			return False
		
		# write it
		with codecs.open(infile, 'w', 'utf-8') as handle:
			# handle.write(unicode(text))
			# handle.write("\n=====\n")
			handle.write(unicode(list_to_sentences(text)))
		
		return True
	
	
	def parse_output(self, filename, **kwargs):
		""" Parse NLTK output. """
		
		if filename is None:
			return None
		
		# is there cTAKES output?
		out_dir = self._out_dir
		if not os.path.exists(out_dir):
			logging.error("The output directory for %s at %s does not exist" % (self.name, out_dir))
			return None
		
		outfile = os.path.join(out_dir, filename)
		if not os.path.exists(outfile):
			# do not log here and silently fail
			return None
		
		tags = []
		
		# read tags
		with codecs.open(outfile, 'r', 'utf-8') as handle:
			#line = handle.readline(keepends=False)		# "keepends" not supported in readline! (http://bugs.python.org/issue8630)
			lines = handle.readlines()
			for line in lines:
				tags.append(line.strip())
		
		# create and return a dictionary (don't filter empty lists)
		ret = {
			'tags': tags,
		}
		
		# clean up
		if self.cleanup:
			os.remove(outfile)
			
			in_dir = self._in_dir
			infile = os.path.join(in_dir, filename)
			if os.path.exists(infile):
				os.remove(infile)
		
		return ret


def _nltk_find_leaves(tree, leave_name):
	try:
		tree.node
	except AttributeError:
		return []
	
	res = []
	if leave_name == tree.node:
		res.append(tree)
	else:
		for child in tree:
			leaves = _nltk_find_leaves(child, leave_name)
			if len(leaves) > 0:
				res.extend(leaves)
	
	return res


# we can execute this file to do some testing
if '__main__' == __name__:
	testtext = "History of clincally significant hypogammaglobulinemia, common variable immunodeficiency, or humeral immunodeficiency."
	testfile = 'test.txt'
	
	run_dir = os.path.join(os.path.dirname(__file__), 'nltk-tags-test')
	my_nlp = NLTKTags({'root': run_dir, 'cleanup': True})
	my_nlp.prepare()
	
	# create test input
	if not my_nlp.write_input(testtext, testfile):
		print "xx>  Failed to write test input to file"
	
	# run
	try:
		my_nlp.run()
	except Exception, e:
		print "xx>  Failed: %s" % e
	
	# parse output
	ret = my_nlp.parse_output(testfile)
	print ret
	
	# clean up
	os.rmdir(my_nlp._in_dir)
	os.rmdir(my_nlp._out_dir)
	os.rmdir(run_dir)
	
	print "-->  Done"
