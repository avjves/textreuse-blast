class DataPreparer:
	
	def __init__(self, data_location, output_folder, threads, langauge):
		self.data_location = data_location
		self.output_folder = output_folder
		self.threads = threads
		self.language = language
		
	def make_directory(self, where):
		if not os.path.exists(where):
			os.makedirs(where)
		
	
	def prepare_data(self):
		self.initial_setup(self.output_folder)
	
				
	## Make intial folders for later	
	def initial_setup(self, where):
		logging.info("Performing initial setups...")
		self.make_directory(where)
		for location in ["encoded", "db", "subgraphs", "info", "batches", "clusters"]:
			self.make_directory(where + "/" + location)
			