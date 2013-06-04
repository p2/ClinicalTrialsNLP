#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#	utilities to handle UMLS
#
#	2013-01-01	Created by Pascal Pfiffner
#


import csv
import sys
import os.path
import logging

from sqlite import SQLite


class UMLS(object):
	""" A class for importing UMLS terminologies into an SQLite database.
	"""
	
	@classmethod
	def check_databases(cls):
		""" Check if our databases are in place and if not, import them.
		
		UMLS: (umls.db)
		If missing prompt to use the `umls.sh` script
		
		SNOMED: (snomed.db)
		Read SNOMED CT from tab-separated files and create an SQLite database.
		"""
		
		# UMLS
		umls_db = os.path.join('databases', 'umls.db')
		if not os.path.exists(umls_db):
			logging.error("The UMLS database at %s does not exist. Run the import script `umls.sh`." % umls_db)
			sys.exit(1)
		
		# SNOMED
		SNOMED.setup_tables()
		map = {
			'descriptions': 'snomed_desc.csv',
			'relationships': 'snomed_rel.csv'
		}
		
		# need to import?
		for table, filename in map.iteritems():
			num_query = 'SELECT COUNT(*) FROM %s' % table
			num_existing = SNOMED.sqlite_handle.executeOne(num_query, ())[0]
			if num_existing > 0:
				continue
			
			snomed_file = os.path.join('databases', filename)
			if not os.path.exists(snomed_file):
				logging.warning("Need to import SNOMED, but the file %s is not present" % filename)
				continue
			
			SNOMED.import_csv_into_table(snomed_file, table)



class UMLSLookup (object):
	""" UMLS lookup """
	
	sqlite_handle = None
	
	
	def __init__(self):
		self.sqlite = SQLite.get('databases/umls.db')
	
	def lookup_code_meaning(self, cui):
		""" Return a string (an empty string if the cui is null or not found)
		by looking it up in our "descriptions" database.
		A lookup in our "descriptions" table is much faster than combing
		through the full MRCONSO table.
		"""
		if cui is None or len(cui) < 1:
			return ''
		
		sql = 'SELECT STR, SAB, TTY FROM descriptions WHERE CUI = ?'
		names = []
		
		for res in self.sqlite.execute(sql, (cui,)):
			names.append("%s (<span style=\"color:#090;\">%s</span>: %s)" % (res[0], res[1], res[2]))
		
		return "<br/>\n".join(names) if len(names) > 0 else ''

	

class SNOMED (object):
	sqlite_handle = None
	
	# -------------------------------------------------------------------------- Database Setup
	@classmethod
	def import_csv_into_table(cls, snomed_file, table_name):
		""" Import SNOMED CSV into our SQLite database.
		The SNOMED CSV files can be parsed by Python's CSV parser with the
		"excel-tab" flavor.
		"""
		
		logging.debug('..>  Importing SNOMED %s into snomed.db...' % table_name)
		
		# not yet imported, parse tab-separated file and import
		with open(snomed_file, 'rb') as csv_handle:
			cls.sqlite_handle.isolation_level = 'EXCLUSIVE'
			sql = cls.insert_query_for(table_name)
			reader = unicode_csv_reader(csv_handle, dialect='excel-tab')
			i = 0
			try:
				for row in reader:
					if i > 0:			# first row is the header row
						
						# execute SQL (we just ignore duplicates)
						params = cls.insert_tuple_from_csv_row_for(table_name, row)
						try:
							cls.sqlite_handle.execute(sql, params)
						except Exception as e:
							sys.exit(u'Cannot insert %s: %s' % (params, e))
					i += 1
				
				# commit to file
				cls.sqlite_handle.commit()
				cls.did_import(table_name)
				cls.sqlite_handle.isolation_level = None
			
			except csv.Error as e:
				sys.exit('CSV error on line %d: %s' % (reader.line_num, e))

		logging.debug('..>  %d concepts parsed' % (i-1))


	@classmethod
	def setup_tables(cls):
		""" Creates the SQLite tables we need, not the tables we deserve.
		"""
		if cls.sqlite_handle is None:
			cls.sqlite_handle = SQLite.get('databases/snomed.db')
		
		# descriptions
		cls.sqlite_handle.create('descriptions', '''(
				concept_id INTEGER PRIMARY KEY,
				lang TEXT,
				term TEXT,
				isa VARCHAR,
				active INT
			)''')
		cls.sqlite_handle.execute("CREATE INDEX IF NOT EXISTS isa_index ON descriptions (isa)")
		
		# relationships
		cls.sqlite_handle.create('relationships', '''(
				relationship_id INTEGER PRIMARY KEY,
				source_id INT,
				destination_id INT,
				rel_type INT,
				rel_text VARCHAR,
				active INT
			)''')
		cls.sqlite_handle.execute("CREATE INDEX IF NOT EXISTS source_index ON relationships (source_id)")
		cls.sqlite_handle.execute("CREATE INDEX IF NOT EXISTS destination_index ON relationships (destination_id)")
		cls.sqlite_handle.execute("CREATE INDEX IF NOT EXISTS rel_type_index ON relationships (rel_type)")
		cls.sqlite_handle.execute("CREATE INDEX IF NOT EXISTS rel_text_index ON relationships (rel_text)")
		
	
	@classmethod
	def insert_query_for(cls, table_name):
		""" Returns the insert query needed for the given table
		"""
		if 'descriptions' == table_name:
			return '''INSERT OR IGNORE INTO descriptions
						(concept_id, lang, term, isa, active)
						VALUES
						(?, ?, ?, ?, ?)'''
		if 'relationships' == table_name:
			return '''INSERT OR IGNORE INTO relationships
						(relationship_id, source_id, destination_id, rel_type, active)
						VALUES
						(?, ?, ?, ?, ?)'''
		return None
	
	
	@classmethod
	def insert_tuple_from_csv_row_for(cls, table_name, row):
		if 'descriptions' == table_name:
			isa = ''
			if len(row) > 6:
				if '900000000000013009' == row[6]:
					isa = 'synonym'
				elif '900000000000003001' == row[6]:
					isa = 'full'
			return (int(row[4]), row[5], row[7], isa, int(row[2]))
		if 'relationships' == table_name:
			return (int(row[0]), int(row[4]), int(row[5]), int(row[7]), int(row[2]))
		return None
	
	
	@classmethod
	def did_import(cls, table_name):
		""" Allows us to set hooks after tables have been imported
		"""
		if 'relationships' == table_name:
			cls.sqlite_handle.execute('''
				UPDATE relationships SET rel_text = 'isa' WHERE rel_type = 116680003
			''')
			cls.sqlite_handle.execute('''
				UPDATE relationships SET rel_text = 'finding_site' WHERE rel_type = 363698007
			''')



class SNOMEDLookup (object):
	""" SNOMED lookup """
	
	sqlite_handle = None
	
	
	def __init__(self):
		self.sqlite = SQLite.get('databases/snomed.db')
	
	def lookup_code_meaning(self, snomed_id):
		if snomed_id is None or len(snomed_id) < 1:
			return ''
		
		sql = 'SELECT term, isa FROM descriptions WHERE concept_id = ?'
		names = []
		
		for res in self.sqlite.execute(sql, (snomed_id,)):
			if 'synonym' == res[1]:
				names.append("<span style=\"color:#888;\">%s</span>" % res[0])
			else:
				names.append(res[0])
		
		return "<br/>\n".join(names) if len(names) > 0 else ''



# the standard Python CSV reader can't do unicode, here's the workaround
def unicode_csv_reader(utf8_data, dialect=csv.excel, **kwargs):
	csv_reader = csv.reader(utf8_data, dialect=dialect, **kwargs)
	for row in csv_reader:
		yield [unicode(cell, 'utf-8') for cell in row]

