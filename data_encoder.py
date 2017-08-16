import argparse, logging, os, sys, json, gzip
#from blast import SingleBlastRunner
from joblib import Parallel, delayed
from text_encoder import TextEncoder

class DataEncoder:
	
	def __init__(self, data_location, output_folder, threads, language="FIN"):
		self.data_location = data_location
		self.output_folder = output_folder
		self.threads = threads
		self.encoder = TextEncoder(language)

	## Encode data into proteins
	def encode_data(self):
		logging.info("Encoding data to proteins...")
		if os.path.isdir(self.data_location):
			files = os.listdir(self.data_location)
			files = [self.data_location + "/" + f for f in files]
		else:
			files = [self.data_location]
		
		Parallel(n_jobs=self.threads)(delayed(self.encode_file)(filename) for filename in files)
	
	## Encode a single file	
	def encode_file(self, filename):
		meta_info = {}
		with open(filename, "r") as data_file, open(self.output_folder + "/encoded/" + filename, "w") as protein_file:
			for line in data_file:
				if not line: continue
				block = json.loads(line.strip())
				text = self.encoder.encode_text(block["text"])
				id = block["id"]
				year = block["year"]
				title = block["title"]
				meta_info[id] = {"title": title, "year": year}
				protein_info = {"text": text, "id": id}
				protein_file.write(json.dumps(protein_info) + "\n")
				
		with open(self.output_folder + "/info/" + filename, "w") as info_file:
			json.dump(meta_info, info_file)
			
			
if __name__ == "__main__":
	
	##TODO Make argparser
	
	where = sys.argv[1]
	data_location = sys.argv[2]
	output = sys.argv[3]
	threads = sys.argv[4]
	runner = BlastRunner(data_location, output, None, None, threads)
	runner.initial_setup()
	encoder = DataEncoder(data_location, output, threads)
	encoder.encode_data()
	runner.generate_db()