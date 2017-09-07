import argparse, logging, os, sys, json, gzip, lmdb, math
#from blast import SingleBlastRunner
from joblib import Parallel, delayed
from text_encoder import TextEncoder

class DataEncoder:
	
	def __init__(self, data_location, output_folder, threads, language="FIN"):
		self.data_location = data_location
		self.output_folder = output_folder
		self.threads = threads
		self.language = language

	## open DB, get keys, start X threads, encode data into gz files, combine into new DB, rejoice
	
	def encode_data(self):
		logging.info("Encoding data to proteins...")
		text_db = self.open_database("original_data_DB")
		keys = self.get_keys(text_db)
		Parallel(n_jobs=self.threads)(delayed(self.encode_text)(key_list, index) for index, key_list in enumerate(keys))
		self.make_encoded_data_DB()
		
	def open_database(self, name, new=False):
		if not new:
			db = lmdb.open(self.output_folder + "/db/" + name, readonly=True)
		else:
			db = lmdb.open(self.output_folder + "/db/" + name, map_size=50000000000)
		return db
	
	## iterate keys, cut into X (num of threads) slices
	def get_keys(self, db):
		with db.begin() as txn:
			keys = [key for key, _ in txn.cursor()]
		key_list = []
		slice_size = math.ceil(len(keys) / self.threads)
		for i in range(0, len(keys), slice_size):
			key_list.append(keys[i:i+slice_size])
		return key_list
	
	def encode_text(self, keys, file_index):
		db = self.open_database("original_data_DB")
		encoder = TextEncoder(self.language)
		with db.begin() as txn, gzip.open(self.output_folder + "/encoded/f_{}.gz".format(file_index), "wt") as gzip_file:
			for key in keys:
				d = {}
				text = txn.get(key).decode("unicode-escape")
				d["id"] = key.decode("utf-8")
				d["text"] = encoder.encode_text(text)
				if len(d["text"]) == 0: continue
				gzip_file.write(json.dumps(d) + "\n")

	def make_encoded_data_DB(self):
		db = self.open_database("encoded_data_DB", new=True)
		with db.begin(write=True) as txn:
			for filename in os.listdir(self.output_folder + "/encoded/"):
				with gzip.open(self.output_folder + "/encoded/" + filename, "rt") as gzf:
					for line in gzf:
						if not line: continue
						block = json.loads(line)
						key, text = block["id"].encode("utf-8"), block["text"].encode("unicode-escape")
						txn.put(key, text)
						
	## Encode data into proteins
	## TODO REMOVE OLD IF NEW WORKS :D
	
	# def encode_data(self):
	# 	logging.info("Encoding data to proteins...")
	# 	if os.path.isdir(self.data_location):
	# 		files = os.listdir(self.data_location)
	# 		folder = self.data_location
	# 	else:
	# 		files = [self.data_location.split("/")[1]]
	# 		folder = self.data_location.split("/")[0]
	# 	
	# 	Parallel(n_jobs=self.threads)(delayed(self.encode_file)(filename, folder) for filename in files)
	# 
	# ## Encode a single file	
	# def encode_file(self, filename, folder):
	# 	meta_info = {}
	# 	with gzip.open(folder + "/" + filename, "rt") as data_file, open(self.output_folder + "/encoded/" + filename, "w") as protein_file:
	# 		for line in data_file:
	# 			if not line: continue
	# 			block = json.loads(line.strip())
	# 			if len(block["text"]) == 0: continue
	# 			text = self.encoder.encode_text(block["text"])
	# 			doc_id = block["doc_id"]
	# 			year = block["year"]
	# 			title = block["title"]
	# 			meta_info[doc_id] = {"title": title, "year": year}
	# 			protein_info = {"text": text, "id": doc_id}
	# 			protein_file.write(json.dumps(protein_info) + "\n")
	# 			
	# 	with open(self.output_folder + "/info/" + filename, "w") as info_file:
	# 		json.dump(meta_info, info_file)
	# 		
			
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