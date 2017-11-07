import os, gzip, json, argparse, lmdb
from joblib import Parallel, delayed
from text_encoder import TextEncoder

class ClusterFiller:

	def __init__(self, output_folder, threads, language):
		self.output_folder=output_folder
		self.threads=threads
		self.encoder = TextEncoder(language)
		self.cluster_iteration = 0

	def fill_clusters(self):
		cluster_files = os.listdir(self.output_folder + "/clusters/unfilled/iteration_{}".format(self.cluster_iteration))
		cluster_files = [f for f in cluster_files if f.endswith(".gz")] ## no folders here!
		Parallel(n_jobs=self.threads)(delayed(self.fill_cluster)(filename, index) for index, filename in enumerate(cluster_files))

	def fill_cluster(self, filename, file_index):
		orig_db = lmdb.open(self.output_folder + "/db/original_data_DB", readonly=True)
		info_db = lmdb.open(self.output_folder + "/db/info_DB", readonly=True)
		with orig_db.begin() as o_db, info_db.begin() as i_db:
			with gzip.open(self.output_folder + "/clusters/unfilled/iteration_{}/".format(self.cluster_iteration) + filename, "rt") as gzip_file:
				data = json.loads(gzip_file.read())
			filled_clusters = {}
			for cluster_key, cluster_data in data.items():
				filled_clusters[cluster_key] = self.fill(cluster_data, o_db, i_db)

		self.save_clusters(filled_clusters, file_index)


	def fill(self, data, o_db, i_db):
		cluster = {}
		length = 0
		hits = []
		for node in data[0]:
			text_id = node.split("___")[0]
			indexes = node.split("___")[1].split("_")
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

	args = parser.parse_args()

	cf = ClusterFiller(args.output_folder, args.threads, args.language)
	cf.fill_clusters()
