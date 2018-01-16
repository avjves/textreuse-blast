import os, gzip, json, argparse, lmdb
from joblib import Parallel, delayed
from natsort import natsorted
from text_encoder import TextEncoder

class ClusterFiller:

	def __init__(self, output_folder, threads, language, split_size):
		self.output_folder=output_folder
		self.threads=threads
		self.split_size = split_size
		self.encoder = TextEncoder(language)

	def fill_clusters(self):
		folders = natsorted(os.listdir("{}/clusters/unfilled".format(self.output_folder)))
		highest_round = folders[-1].split("_")[1]
		right_folders = [f for f in folders if "round_{}".format(highest_round) in f]
		files = []
		for right_folder in right_folders:
			right_files = os.listdir("{}/clusters/unfilled/{}".format(self.output_folder, right_folder))
			for right_file in right_files:
				files.append((right_folder, right_file))

		#cluster_files = ["{}/clusters/unfilled/{}/{}".format(self.output_folder, folders[-1], f) for f in os.listdir("{}/clusters/unfilled/{}".format(self.output_folder, folders[-1]))]

		#cluster_files = os.listdir(self.output_folder + "/clusters/unfilled/iteration_{}".format(self.cluster_iteration))
		#cluster_files = [f for f in cluster_files if f.endswith(".gz")] ## no folders here!
		Parallel(n_jobs=self.threads)(delayed(self.fill_cluster)(folder, filename, index) for index, (folder, filename) in enumerate(files))

	def fill_cluster(self, folder, filename, file_index):
		orig_db = lmdb.open(self.output_folder + "/db/original_data_DB", readonly=True)
		info_db = lmdb.open(self.output_folder + "/db/info_DB", readonly=True)
		with orig_db.begin() as o_db, info_db.begin() as i_db:
			with gzip.open(self.output_folder + "/clusters/unfilled/" + folder + "/" + filename, "rt") as gzip_file:
				data = json.loads(gzip_file.read())
			filled_clusters = {}
			for cluster_key, cluster_data in data.items():
				filled_clusters[cluster_key] = self.fill(cluster_data, o_db, i_db)

		self.save_clusters(filled_clusters, file_index)

	def generate_split_indexes(self, indexes):
		self.split_size = int(self.split_size)
		end = (int(int(indexes[0]) / self.split_size) + 1) * self.split_size
		start = int(int(indexes[1]) / self.split_size) * self.split_size
		return start, end


	def fill(self, data, o_db, i_db):
		cluster = {}
		length = 0
		hits = []
		for node in data[0]:
			text_id = node.split("___")[0]
			indexes = node.split("___")[1].split("_")
			if self.split_size != None:
				doc_start, doc_end = self.generate_split_indexes(indexes)
				text_id = "{}__{}_{}".format(text_id, doc_start, doc_end)

			orig_text = self.get_original_text(text_id, o_db)
			text, indices = self.encoder.decode_text(orig_text, indexes[0], indexes[1])
			hit_data = {}
			length += len(text)
			key = node.split("___")[0]
			info = json.loads(i_db.get(key.encode("utf-8")).decode("unicode_escape"))
			for key, value in info.items():
				hit_data[key] = value

			hit_data["text"] = text
			hit_data["node"] = node
			hit_data["doc_id"] = key
			hit_data["original_indices"] = indices
			hit_data["encoded_indices"] = indexes
			hits.append(hit_data)

		cluster["length"] = int(length/len(hits))
		cluster["hits"] = hits
		return cluster

	def get_original_text(self, text_id, db):
		return db.get(text_id.encode("utf-8")).decode("unicode-escape")

	def save_clusters(self, clusters, file_index):
		with gzip.open(self.output_folder + "/clusters/filled/clusters_{}.gz".format(file_index), "wt") as gzf:
			gzf.write(json.dumps(clusters))


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Filling the clusters with the actual text.")
	parser.add_argument("--threads", help="Number of threads to use", default=1, type=int)
	parser.add_argument("--output_folder", help="A folder where all data will be stored in the end.", required=True)
	parser.add_argument("--language", help="Encoding language", default="FIN")
	parser.add_argument("--split_size", help="Split size if splitting was used to prepare the data.", default=None)

	args = parser.parse_args()

	cf = ClusterFiller(args.output_folder, args.threads, args.language, args.split_size)
	cf.fill_clusters()
