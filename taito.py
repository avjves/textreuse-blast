import argparse, os
from blast import MultipleBlastRunner

''' to run on a cluster computer, it might be helpful to copy db to the hardrive of the node
	This file can be run instead of blast.py. Will make sure the data is on the node before running
	blast.py'''
if __name__ == "__main__":
	
	
	def __init__(self, output_folder, e_value, word_size, threads, iter, max_iter, queries_per_iter, text_count):
		self.output_folder=output_folder
		self.e_value=e_value
		self.word_size=word_size
		self.threads=threads
		self.iter = iter
		self.max_iter = max_iter
		self.queries_per_iter = queries_per_iter
		self.text_count = text_count
		self.db_loc = output_folder + "/db/textdb"
	
	
	runner = SingleBlastRunner(output_folder=output_folder, e_value=e_value, word_size=word_size, threads=threads, iter=iter, max_iter=max_iter, queries_per_iter=queries_per_iter, text_count=text_count)
	runner.run()