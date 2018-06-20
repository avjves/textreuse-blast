import os, gzip, json, argparse, lmdb
from joblib import Parallel, delayed
from natsort import natsorted
from text_encoder import TextEncoder
from tqdm import tqdm

class ClusterFiller:

	def __init__(self, output_folder, threads, language, split_size, data_dbs, info_dbs, custom_unfilled, custom_filled, min_count):
		self.output_folder=output_folder
		self.threads=threads
		self.split_size = split_size
		self.encoder = TextEncoder(language)
		self.min_count = min_count
		self.custom_unfilled = custom_unfilled
		if data_dbs == None:
			self.custom_data_DBs = None
		else:
			self.custom_data_DBs = data_dbs.split(";")
		if info_dbs == None:
			self.custom_info_DBs = None
		else:
			self.custom_info_DBs = info_dbs.split(";")
		if custom_unfilled == None:
			self.cluster_folder = "{}/clusters/unfilled".format(self.output_folder)
		else:
			self.cluster_folder = custom_unfilled
			if not os.path.exists(self.cluster_folder):
				os.makedirs(self.cluster_folder)
		if custom_filled == None:
			self.save_folder = "{}/clusters/filled".format(self.output_folder)
		else:
			self.save_folder = custom_filled
			if not os.path.exists(self.save_folder):
				os.makedirs(self.save_folder)

	def fill_clusters(self):
		if self.custom_unfilled == None:
			folders = natsorted(os.listdir(self.cluster_folder))
			if len(folders) == 1:
				right_folders = folders
			else:
				highest_round = folders[-1].split("_")[1]
				right_folders = [f for f in folders if "round_{}".format(highest_round) in f]
			files = []
			for right_folder in right_folders:
				right_files = os.listdir("{}/{}".format(self.cluster_folder, right_folder))
				for right_file in right_files:
					files.append((right_folder, right_file))
		else:
			files = []
			cfiles = natsorted(os.listdir(self.cluster_folder))
			for cfile in cfiles:
				files.append(("", cfile))
		Parallel(n_jobs=self.threads)(delayed(self.fill_cluster)(folder, filename, index) for index, (folder, filename) in tqdm(enumerate(files)))

	def fill_cluster(self, folder, filename, file_index):
		if self.custom_data_DBs == None:
			use_custom_dbs = False
			orig_db = lmdb.open(self.output_folder + "/db/original_data_DB", readonly=True)
			info_db = lmdb.open(self.output_folder + "/db/info_DB", readonly=True)
			o_db = orig_db.begin()
			i_db = [info_db.begin()]
		else:
			o_db = []
			i_db = []
			use_custom_dbs = True
			for db_path in self.custom_data_DBs:
				db = lmdb.open(db_path, readonly=True)
				o_db.append(db.begin())
			for db_path in self.custom_info_DBs:
				db = lmdb.open(db_path, readonly=True)
				i_db.append(db.begin())

		with gzip.open(self.cluster_folder + "/" + folder + "/" + filename, "rt") as gzip_file:
			data = json.loads(gzip_file.read())
		filled_clusters = {}
		for cluster_key, cluster_data in data.items():
			if len(cluster_data[0]) > self.min_count:
				filled_clusters[cluster_key] = self.fill(cluster_data, o_db, i_db, use_custom_dbs)
		self.save_clusters(filled_clusters, file_index)

	def generate_split_indexes(self, indexes):
		self.split_size = int(self.split_size)
		end = (int(int(indexes[0]) / self.split_size) + 1) * self.split_size
		start = int(int(indexes[1]) / self.split_size) * self.split_size
		return start, end


	def fill(self, data, o_db, i_db, use_custom_dbs):
		cluster = {}
		length = 0
		hits = []
		skips = []
		for node in data[0]:
			text_id = node.split("___")[0]
			indexes = node.split("___")[1].split("_")[0:2]
			if self.split_size != None and self.split_size > 0:
				doc_start, doc_end = self.generate_split_indexes(indexes)
				text_id = "{}__{}_{}".format(text_id, doc_start, doc_end)

			if "_ext" in node:
				i = 0
				base = text_id.split("_")[0]
				texts = []
				while True:
					t = self.get_original_text("{}_{}_{}".format(base, i*10000, (i+1)*10000), o_db, use_custom_dbs)
					if t == None: break
					texts.append(t)
					i += 1
				try:
					text, indices = self.encoder.decode_text("".join(texts), indexes[0], indexes[1])
				except IndexError:
					print(node, indexes)
					skips.append(node)
					continue
			else:
				orig_text = self.get_original_text(text_id, o_db, use_custom_dbs)
				try:
					text, indices = self.encoder.decode_text(orig_text, indexes[0], indexes[1])
				except IndexError:
					print(node, indexes)
					skips.append(node)
					continue
			hit_data = {}
			length += len(text)
			doc_key = node.split("___")[0]
			for info_db in i_db:
				info = info_db.get(doc_key.encode("utf-8"))
				if info == None: continue
				else:
					info = json.loads(info.decode("unicode-escape"))

				for key, value in info.items():
					hit_data[key] = value


			hit_data["text"] = text
			hit_data["node"] = node
			hit_data["doc_id"] = doc_key
			hit_data["original_indices"] = indices
			hit_data["encoded_indices"] = indexes
			hits.append(hit_data)

		cluster["length"] = int(length/len(hits))
		cluster["hits"] = hits
		cluster["skips"] = skips
		return cluster

	def get_original_text(self, text_id, db, use_custom_dbs):
		if use_custom_dbs:
			text = None
			for custom_db in db:
				text = custom_db.get(text_id.encode("ascii"))
				if text != None: break
				text = custom_db.get(text_id.replace("__", "_").encode("ascii"))
				if text != None: break
		else:
			text = db.get(text_id.encode("utf-8"))
		if text == None:
			return None
		else:
			return text.decode("unicode-escape")

	def save_clusters(self, clusters, file_index):
		if len(clusters) != 0:
			with gzip.open("{}/clusters_{}.gz".format(self.save_folder, file_index), "wt") as gzf:
				gzf.write(json.dumps(clusters))


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Filling the clusters with the actual text.")
	parser.add_argument("--threads", help="Number of threads to use", default=1, type=int)
	parser.add_argument("--output_folder", help="A folder where all data will be stored in the end.", required=True)
	parser.add_argument("--language", help="Encoding language", default="FIN")
	parser.add_argument("--split_size", help="Split size if splitting was used to prepare the data.", default=None)
	parser.add_argument("--custom_data_DBs", help="If you want to use custom data DBs. Specify the paths like 'path1;path2'", default=None)
	parser.add_argument("--custom_info_DBs", help="If you want to use custom info DBs. Specify the paths like 'path1;path2'", default=None)
	parser.add_argument("--custom_unfilled", help="Custom location of unfilled files.")
	parser.add_argument("--custom_filled", help="Custom location of to-be filled files.")
	parser.add_argument("--min_count", help="Min count. Dev option.", type=int, default=0)

	args = parser.parse_args()
	print("Arguments:")
	print(args)

	cf = ClusterFiller(args.output_folder, args.threads, args.language, args.split_size, args.custom_data_DBs, args.custom_info_DBs, args.custom_unfilled, args.custom_filled, args.min_count)
	cf.fill_clusters()
