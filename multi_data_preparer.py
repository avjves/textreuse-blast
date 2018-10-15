from data_encoder import DataEncoder
import argparse, os, json, subprocess, gzip, lmdb

from data_preparer import DataPreparer
from text_logging import get_logger

class MultipleDataPreparer(DataPreparer):

	def __init__(self, data_folder, output_folder, threads, language, split_size, logger):
		self.data_folder = data_folder
		self.output_folder = output_folder
		self.language = language
		self.split_size = split_size
		self.threads = threads
		self.gi = 1
		self.text_counts = []
		self.logger = logger

	def prepare_data(self):
		self.initial_setup(self.output_folder)
		self.data_to_lmdb()
		self.data_encoder.encode_data()
		self.generate_db()


	def prepare_data(self):
		self.initial_setup(self.output_folder)
		data_folders = self.extract_data_folders()
		for data_folder in data_folders:
			data_folder_path, data_folder_name = data_folder
			self.data_to_lmdb(data_folder_name, data_folder_path)
			data_encoder = DataEncoder(data_folder_path, self.output_folder, self.threads, self.language, data_folder_name)
			data_encoder.encode_data()
			self.make_fasta_file(data_folder_name)
			self.clean_encoded()
			self.logger.info("Added DB: {} \t {} new texts".format(data_folder_name, self.text_counts[-1][1]))
		self.save_text_counts()
		self.make_db()

	def open_databases(self, name):
		text_env = lmdb.open(self.output_folder + "/db/original_data_{}_DB".format(name), map_size=500000000000)
		info_env = lmdb.open(self.output_folder + "/db/info_{}_DB".format(name), map_size=50000000000)
		return text_env, info_env


	def extract_data_folders(self):
		if self.data_folder == None:
			return []
		else:
			splits = self.data_folder.split(";")
			return [(splits[i], splits[i+1]) for i in range(0, len(splits), 2)]

	def get_data_files(self, path):
		if os.path.isdir(path):
			files = os.listdir(path)
			folder = path
		else:
			files = [path.split("/")[1]]
			folder = path.split("/")[0]

		return files, folder

	## Generate the protein database for BLAST
	def generate_db(self, data_folder_name, data_folder_path):
		self.logger.info("Generating {} protein database..".format(data_folder_name))
		self.make_fasta_file(data_folder_name)
		self.make_db()

	def clean_encoded(self):
		for filename in os.listdir(self.output_folder + "/encoded"):
			os.remove(self.output_folder + "/encoded/" + filename)

	def save_text_counts(self):
		with gzip.open(self.output_folder + "/info/text_counts.gz", "wt") as gzf:
			gzf.write(json.dumps(self.text_counts))


	def make_fasta_file(self, data_folder_name):
		encoded_db = lmdb.open(self.output_folder + "/db/encoded_data_{}_DB".format(data_folder_name), readonly=True)
		begin_gi = self.gi
		with encoded_db.begin() as db:
			with open(self.output_folder + "/db/database.fsa", "a") as fasta_file:
				for key, value in db.cursor():
					doc_id = key.decode("utf-8")
					text = value.decode("unicode-escape")
					begin = ">gi|{} {}".format(begin_gi, doc_id)
					fasta_file.write("{}\n{}\n".format(begin, text))
					begin_gi += 1
		self.text_counts.append((data_folder_name, (begin_gi - self.gi)))
		self.gi = begin_gi

	def get_text_count(self):
		return self.text_count

	## Make the DB using makeblastdb
	def make_db(self):
		subprocess.call("makeblastdb -in {} -dbtype prot -title TextDB -parse_seqids -hash_index -out {}".format(self.output_folder + "/db/database.fsa", self.output_folder + "/db/textdb").split(" "))
		self.db_loc = self.output_folder + "/db/textdb"



	## Generate a LMDB DB for the original data. Helps in reconstructing phase, this is done with just ONE thread :c
	def data_to_lmdb(self, data_folder_name, data_folder_path):
		self.logger.info("Loading original data from {} into databases...".format(data_folder_name))
		text_db, info_db = self.open_databases(data_folder_name)
		files, folder = self.get_data_files(data_folder_path)
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


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Preparing the data. This should only be run if you are planning on running the software in batches.")
	parser.add_argument("--threads", help="Number of threads to use", default=1, type=int)
	parser.add_argument("--data_folders", help="Folders to use. type: path1;name1;path2;name2", required=True)
	parser.add_argument("--output_folder", help="A folder where all data will be stored in the end.", required=True)
	parser.add_argument("--language", help="Encoding language", default="FIN")
	parser.add_argument("--split_size", type=int, help="If needed to split the data prior to entering it into the DB", default=-1)
	args = parser.parse_args()

	logger = get_logger()

	dp = MultipleDataPreparer(args.data_folders, args.output_folder, args.threads, args.language, args.split_size, logger)
	dp.prepare_data()
