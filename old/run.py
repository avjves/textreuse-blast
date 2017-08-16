import argparse, os, sys, json, gzip, codecs
from joblib import Parallel, delayed
import multiprocessing
from text_encoder import TextEncoder
import subprocess


class BlastWorker(object):

	def __init__(self, data, output, size, threads, min_length, max_length, subgraph, tsv, full, evalue, word_size):
		self.data_location = data
		self.encoder = TextEncoder()
		self.output_folder = output
		self.db_size = 0
		self.threads = threads
		self.min_length = min_length
		self.max_length = max_length
		self.evalue = evalue
		self.word_size = word_size
		self.flags = ""
		if subgraph:
			self.flags += " --subgraph"
		if tsv:
			self.flags += " --tsv"
		if full:
			self.flags += " --full"

	def run(self):
		print("Encoding data...")
		self.encode_data()
		print("Making fasta file...")
		self.make_fasta()
		print("Making BLAST database...")
		self.make_blast_db()
		print("Running BLAST...")
		self.run_blast()
		print("Clustering results...")
		self.cluster_results()

	def encode_data(self):
		files = []
		if os.path.isdir(self.data_location):
			files = os.listdir(self.data_location)
			files = [f for f in files if not f.startswith(".")]
		else:
			dl = self.data_location.split("/")
			filename = dl.pop(-1)
			self.data_location = "/".join(dl)
			files.append(filename)

		os.makedirs(self.output_folder + "/encoded")
		Parallel(n_jobs=self.threads)(delayed(self.encode)(filename, self.data_location) for filename in files)
		self.combine_metadata()

	def encode(self, filename, data_location):
		encoded_data = {}
		info = {}
		with open(data_location + "/" + filename) as json_file:
			jdata = json.load(json_file)
			for key, value in jdata.items():
				data = value
				text = data["text"]
				info[key] = {}
				info[key]["year"] = data["year"]
				info[key]["title"] = data["title"]
				info[key]["filename"] = filename
				encoded_data[key] = self.encoder.encode_text(text)
		with gzip.open(self.output_folder + "/encoded/" + filename + ".gz", "wb") as gzip_file:
			gzip_file.write(bytes(json.dumps(encoded_data), "utf-8"))
		try:
			os.makedirs(self.output_folder + "/metadata")
		except FileExistsError:
			pass
		with codecs.open(self.output_folder + "/metadata/" + str(multiprocessing.current_process()), "w") as json_file:
			json.dump(info, json_file)

	def combine_metadata(self):
		metadata = {}
		for filename in os.listdir(self.output_folder + "/metadata/"):
			with codecs.open(self.output_folder + "/metadata/" + filename) as json_file:
				jdata = json.load(json_file)
			metadata.update(jdata)
		with codecs.open(self.output_folder + "/metadata.json", "w") as json_file:
			json.dump(metadata, json_file)

	def make_fasta(self):
		gi = 0
		os.makedirs(self.output_folder + "/database")
		with codecs.open(self.output_folder + "/database/db.fsa", "w") as fasta_file:
			for encoded_file in os.listdir(self.output_folder + "/encoded"):
				with gzip.open(self.output_folder + "/encoded/" + encoded_file, "rb") as gzip_file:
					gdata = json.loads(str(gzip_file.read(), "utf-8"))
				for key, value in gdata.items():
					gi += 1
					fasta_file.write(">gi|" + str(gi) + " " + key + "\n")
					fasta_file.write(value + "\n")

		self.db_size = int(gi)


	def make_blast_db(self):
		os.system("makeblastdb -dbtype prot -parse_seqids -hash_index -title database -out " + self.output_folder + "/database/database -in " + self.output_folder + "/database/db.fsa")


	def run_blast(self):
		os.makedirs(self.output_folder + "/results")
		os.system("blastp -db " + self.output_folder + "/database/database -query " + self.output_folder + "/database/db.fsa -matrix BLOSUM62 -gapopen 3 -gapextend 11 -threshold 400 -word_size " + self.word_size + " -outfmt \"7 stitle qstart qend sstart send length ppos\" -num_threads " + str(self.threads) + " -evalue " + self.evalue + " -out " + self.output_folder + "/results/result.tsv")

	def cluster_results(self):
		os.system("python3 cluster_mcores.py -f " + self.output_folder + "/results/result.tsv -min " + str(self.min_length) + " -max " + str(self.max_length) + " -d " + self.data_location + " -o " + self.output_folder + " --num_of_processes " + str(self.threads) + self.flags)


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Software to find repetitions within texts using BLAST.")
	parser.add_argument("-d", "--data", help="Location to either a single JSON file or a folder with multiple JSON files.", required=True)
	parser.add_argument("--num_process", help="Number of processes to launch when running BLAST and clustering", default=1, type=int)
	parser.add_argument("--out_folder", help="Output folder where all the data will be stored", required=True)
	parser.add_argument("--min_length", help="Minimum length of hits", default=0)
	parser.add_argument("--max_length", help="Maximum length of hits", default=1000000000)
	parser.add_argument("--evalue", help="E-value for BLAST. Lowering this will increase computing time, but will find shorter hits as well, default = 1e-8", default="1e-8")
	parser.add_argument("--word_size", help="Word size for BLAST. Lowering this will increase computing time, but might help finding hits, allowed values 2-7", default="7")
	parser.add_argument("--subgraphs", action="store_true", help="Store subgraphs. So just lists that have the name of the text and the encoded indexes of the hit.", default=False)
	parser.add_argument("--tsv", action="store_true", help="Store TSV-file.", default=False)
	parser.add_argument("--full", action="store_true", help="Store full clusters", default=False)
	parser.add_argument("--compress", action="store_true", help="Compress data in memory while running. Slows down a lot, saves RAM, not working atm", default=False)
	args = parser.parse_args()



	blast_worker = BlastWorker(args.data, args.out_folder, 0, args.num_process, args.min_length, args.max_length, args.subgraphs, args.tsv, args.full, args.evalue, args.word_size)
	blast_worker.run()

	#print("Removing temp data...")
	#os.system("rm -rf " + args.out_folder + "/database " + args.out_folder + "/encoded " + args.out_folder + "/metadata " + args.out_folder + "/results")
