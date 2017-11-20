import os, json, sys, logging, subprocess, gzip
from joblib import Parallel, delayed
from text_encoder import TextEncoder
from data_encoder import DataEncoder
logging.basicConfig(level=0)

def make_directory(self, where):
	if not os.path.exists(where):
		os.makedirs(where)

class SingleBlastRunner:

	def __init__(self, data, output_folder, e_value, word_size, threads, text_count, language="FIN"):
		self.data_location=data
		self.output_folder=output_folder
		self.e_value=e_value
		self.word_size=word_size
		self.threads=threads
		self.text_count = text_count
		self.data_encoder = DataEncoder(data, output_folder, threads, language)



	## Run software in a single process
	def run(self):
		logging.info("Running software...")
	#	self.initial_setup(self.output_folder)
		#self.data_encoder.encode_data()
		#self.generate_db()
		self.run_blast()
		logging.info("BLASTing done...")

	## Generate the protein database for BLAST
	def generate_db(self):
		logging.info("Generating protein database..")
		self.make_fasta_file()
		self.make_db()

	## FASTA file for DB generation
	def make_fasta_file(self):
		with open(self.output_folder + "/db/database.fsa", "w") as fasta_file:
			gi = 1
			encoded_files = os.listdir(self.output_folder + "/encoded")
			for filename in encoded_files:
				with gzip.open(self.output_folder + "/encoded/" + filename, "rt") as encoded_file:
					for line in encoded_file:
						block = json.loads(line)
						id = block["id"]
						text = block["text"]
						begin = ">gi|{}| {}".format(gi, id)
						fasta_file.write("{}\n{}\n".format(begin, text))
						gi += 1
		self.text_count = gi

	## Make the DB using makeblastdb
	def make_db(self):
		subprocess.call("makeblastdb -in {} -dbtype prot -title TextDB -parse_seqids -hash_index -out {}".format(self.output_folder + "/db/database.fsa", self.output_folder + "/db/textdb").split(" "))
		self.db_loc = self.output_folder + "/db/textdb"

	## Run blast, one query at a time
	def run_blast(self):
		logging.info("Running BLAST...")
		for i in range(1, self.text_count):
			self.generate_positive_gi_list(i)
			self.make_query_file(i)
			subprocess.call(["blastp", "-db", self.output_folder + "/db/textdb", "-query", self.query_loc, "-gilist", self.gi_loc, "-out", self.output_folder + "/batches/batch_" + str(i) + ".tsv", "-evalue", str(self.e_value), "-word_size", str(self.word_size), "-gapopen", "3", "-gapextend", "11", "-matrix", "BLOSUM62", "-threshold", "400", "-outfmt", "7 stitle qstart qend sstart send length ppos", "-num_threads", str(self.threads)])
	## List of queries to compare against, so two queries will never be queried against each other twice
	def generate_positive_gi_list(self, index):
		with open(self.output_folder + "/info/pos_gi.txt", "w") as gi_list:
			for i in range(index+1, self.text_count+1):
				gi_list.write("{}\n".format(i))
		self.gi_loc = self.output_folder + "/info/pos_gi.txt"

	## Get the query from the DB
	def make_query_file(self, gi_index):
		with open(self.output_folder + "/info/query.fsa", "w") as query_file:
			subprocess.call("blastdbcmd -db {} -entry {}".format(self.output_folder + "/db/textdb", gi_index).split(" "), stdout=query_file)
		self.query_loc = self.output_folder + "/info/query.fsa"

## When running the software in batches
class MultipleBlastRunner:

	def __init__(self, output_folder, e_value, word_size, threads, iter, queries_per_iter, text_count):
		self.output_folder=output_folder
		self.e_value=e_value
		self.word_size=word_size
		self.threads=threads
		self.iter = iter
		self.queries_per_iter = queries_per_iter
		self.text_count = text_count
		self.db_loc = output_folder + "/db/textdb"


	def run(self):
		logging.info("Running software...")
		self.make_iteration_folders()
		self.run_blast()
		self.compress_results()
		logging.info("Blasting done...")


	def make_iteration_folders(self):
		folders_to_make = ["{}/info/iter_{}", "{}/batches/iter_{}"]
		for folder in folders_to_make:
			if not os.path.exists(folder.format(self.output_folder, self.iter)):
				os.makedirs(folder.format(self.output_folder, self.iter))

	def generate_positive_gi_list(self, index):
		begin = 1 + self.iter*self.queries_per_iter
		with open("{}/info/iter_{}/pos_gi.txt".format(self.output_folder, self.iter), "w") as gilist:
			for i in range(begin+index, self.text_count+1):
				gilist.write("{}\n".format(i))
		self.gi_loc = "{}/info/iter_{}/pos_gi.txt".format(self.output_folder, self.iter)

	def make_query_file(self, index):
		gi_index = 0 + self.iter*self.queries_per_iter + index
		with open(self.output_folder + "/info/iter_{}/query.fsa".format(self.iter), "w") as query_file:
			subprocess.call("blastdbcmd -db {} -entry {}".format("{}/db/textdb".format(self.output_folder), gi_index).split(" "), stdout=query_file)
		self.query_loc = "{}/info/iter_{}/query.fsa".format(self.output_folder, self.iter)

	def run_blast(self):
		logging.info("Running BLAST...")
		for i in range(1, self.queries_per_iter+1):
			logging.info("Running query: #{}".format(i))
			self.generate_positive_gi_list(i)
			self.make_query_file(i)
			subprocess.call(["blastp", "-db", self.db_loc, "-query", self.query_loc, "-gilist", self.gi_loc, "-out", self.output_folder + "/batches/iter_" + str(self.iter) + "/batch_" + str(i) + ".tsv", "-evalue", str(self.e_value), "-word_size", str(self.word_size), "-gapopen", "3", "-gapextend", "11", "-matrix", "BLOSUM62", "-threshold", "400", "-outfmt", "7 stitle qstart qend sstart send length ppos", "-num_threads", str(self.threads)])

	def compress_results(self):
		logging.info("Compressing results...")
		subprocess.call("tar -zcf {}/iter_{}.tar.gz -C {}/iter_{} . --remove-files --warning none".format(self.output_folder + "/batches", self.iter, self.output_folder + "/batches", self.iter).split(" "))
