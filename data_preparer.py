from data_encoder import DataEncoder
import argparse, logging, os, json, subprocess, gzip, lmdb
logging.basicConfig(level=0)


class DataPreparer:

	def __init__(self, data_location, output_folder, threads, language, split_size):
		self.data_location = data_location
		self.output_folder = output_folder
		self.threads = threads
		self.language = language
		self.data_encoder = DataEncoder(data_location, output_folder, threads, language)
		self.split_size = split_size

	def make_directory(self, where):
		if not os.path.exists(where):
			os.makedirs(where)


	def prepare_data(self):
		self.initial_setup(self.output_folder)
		self.data_to_lmdb()
		self.data_encoder.encode_data()
		self.generate_db()

	def split_text_into_blocks(self, block):
		if self.split_size > 0:
			for i in range(0, len(block["text"]), self.split_size):
				yield block["text"][i:i+self.split_size], str(block["doc_id"]) + "__{}_{}".format(i, i +self.split_size)
		else:
			yield block["text"], str(block["doc_id"])
	## Generate a LMDB DB for the original data. Helps in reconstructing phase, this is done with just ONE thread :c
	def data_to_lmdb(self):
		logging.info("Loading original data into databases...")
		text_db, info_db = self.open_databases()
		files, folder = self.get_data_files()
		with text_db.begin(write=True) as t_db, info_db.begin(write=True) as i_db:
			for filename in files:
				with gzip.open(folder + "/" + filename, "rt") as data_file:
					for line in data_file:
						if not line: continue
						block = json.loads(line.strip())
						if len(block["text"]) == 0: continue
						for block_i, split_block in enumerate(self.split_text_into_blocks(block)):
							text_block, new_id = split_block
							block_data = {}
							for k,v in block.items(): block_data[k] = v
							block_data["text"] = text_block
							## Only ASCII in doc_id! BLAST doesn't like non ascii characters in fsa
							doc_id = new_id.encode("ascii", errors="ignore")
							t_db.put(doc_id, block_data["text"].encode("unicode-escape"))
							del block_data["text"], block_data["doc_id"]
							i_db.put(doc_id, json.dumps(block_data).encode("unicode-escape"))
							##TODO BUT DOES IT WORK?


	def get_data_files(self):
		if os.path.isdir(self.data_location):
			files = os.listdir(self.data_location)
			folder = self.data_location
		else:
			files = [self.data_location.split("/")[1]]
			folder = self.data_location.split("/")[0]

		return files, folder



	def open_databases(self):
		text_env = lmdb.open(self.output_folder + "/db/original_data_DB", map_size=50000000000)
		info_env = lmdb.open(self.output_folder + "/db/info_DB", map_size=5000000000)
		return text_env, info_env

	## Generate the protein database for BLAST
	def generate_db(self):
		logging.info("Generating protein database..")
		self.make_fasta_file()
		self.make_db()


	def make_fasta_file(self):
		encoded_db = lmdb.open(self.output_folder + "/db/encoded_data_DB", readonly=True)
		gi = 1
		with encoded_db.begin() as db:
			with open(self.output_folder + "/db/database.fsa", "w") as fasta_file:
				for key, value in db.cursor():
					doc_id = key.decode("utf-8")
					text = value.decode("unicode-escape")
					begin = ">gi|{} {}".format(gi, doc_id)
					fasta_file.write("{}\n{}\n".format(begin, text))
					gi += 1
		self.text_count = gi-1

	def get_text_count(self):
		return self.text_count

	## Make the DB using makeblastdb
	def make_db(self):
		subprocess.call("makeblastdb -in {} -dbtype prot -title TextDB -parse_seqids -hash_index -out {}".format(self.output_folder + "/db/database.fsa", self.output_folder + "/db/textdb").split(" "))
		self.db_loc = self.output_folder + "/db/textdb"



	## Make intial folders for later
	def initial_setup(self, where):
		logging.info("Performing initial setups...")
		self.make_directory(where)
		for location in ["encoded", "db", "subgraphs", "info", "batches", "clusters", "clusters/unfilled", "clusters/filled"]:
			self.make_directory(where + "/" + location)



if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Preparing the data. This should only be run if you are planning on running the software in batches.")
	parser.add_argument("--threads", help="Number of threads to use", default=1, type=int)
	parser.add_argument("--data_location", help="Location of the data files", required=False)
	parser.add_argument("--output_folder", help="A folder where all data will be stored in the end.", required=True)
	parser.add_argument("--language", help="Encoding language", default="FIN")
	parser.add_argument("--split_size", type=int, help="If needed to split the data prior to entering it into the DB", default=-1)
	args = parser.parse_args()

	dp = DataPreparer(args.data_location, args.output_folder, args.threads, args.language, args.split_size)
	dp.prepare_data()
