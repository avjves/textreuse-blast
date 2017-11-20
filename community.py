from operator import itemgetter
class CommunityDetector:

	def __init__(self, detection_style=None):
		self.detection_style=detection_style

	def detect(self, nodes, edges):
		if self.detection_style == "louvain":
			return self.detect_louvain(nodes, edges)
		else:
			return [self.de_uniq(nodes)]

	def detect_louvain(self, nodes, edges):
		return [[nodes, 0]]

	def de_uniq(self, nodes):
		keys = {}
		new_nodes = []
		length = 0
		for node in nodes:
			key, indexes = node.split("___")
			indexes = [int(v) for v in indexes.split("_")]
			keys[key] = keys.get(key, [])
			keys[key].append(indexes)


		for key, indexes in keys.items():
			new_indexes = self.remove_duplicates(indexes)
			nodes, leng = self.recreate_nodes(key, new_indexes)
			new_nodes += nodes
			length += leng
		return [new_nodes, int(length/len(new_nodes))]


	def recreate_nodes(self, key, indexes):
		nodes, length = [], 0
		for ind in indexes:
			nodes.append("{}___{}_{}".format(key, ind[0][0], ind[0][1]))
			length += (ind[0][1]-ind[0][0])
		return nodes, length

	def remove_duplicates(self, indexes):
		indexes.sort(key=itemgetter(0))
		done = set()
		overlaps = []
		for i in range(len(indexes)):
			if i in done: continue
			done.add(i)
			overlapping = []
			comp = indexes[i]
			overlapping.append([comp, comp[1]-comp[0]])
			for j in range(i+1, len(indexes)):
				if j in done: continue
				curr = indexes[j]
				if curr[0] < comp[1]:
					overlapping.append([curr, curr[1]-curr[0]])
					done.add(j)
			overlaps.append(overlapping)

		newi = []
		for overlap in overlaps:
			overlap.sort(key=itemgetter(1), reverse=True)
			newi.append(overlap[0])
		return newi
