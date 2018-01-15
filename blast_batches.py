import argparse, os, time, subprocess
from shutil import copytree, rmtree, copyfile
from blast import MultipleBlastRunner

''' to run on a cluster computer, it might be helpful to copy db to the hardrive of the node
	This file can be run instead of blast.py. Will make sure the data is on the node before running
	blast.py'''

	## DATA MUST BE PREPARED BEFORE RUNNING THIS


def get_folder_size(folder):
	return int(subprocess.check_output(["du", "-s", folder]).split()[0].decode("utf-8"))


def copy_output_folder_to_local(output_folder, local_folder, wait=True, wait_time=5):
	if wait:
		if os.path.exists(local_folder):
			print("Folder exists.")
			original_size = get_folder_size(output_folder)
			while True:
				current_size = get_folder_size(local_folder)
				print("Current folder size: {}".format(current_size))
				if current_size < original_size:
					print("{} < {}, waiting {} seconds.".format(current_size, original_size, wait_time))
					time.sleep(wait_time)
				else:
					break
		else:
			try:
				copytree(output_folder, local_folder)
			except OSError:
				print("Couldn't copy, wait a while...")
				time.sleep(wait_time)
				copy_output_folder_to_local(output_folder, local_folder, wait, wait_time)
	else:
		copytree(output_folder, local_folder)

#def copy_output_folder_to_local(output_folder, local_folder):
	#copytree(output_folder, local_folder)


def delete_local_data(local_folder):
	rmtree(local_folder)

def copy_local_data_back(output_folder, batch_folder, iter):
	if not os.path.exists(batch_folder):
		os.path.makedirs(batch_folder)
	copyfile(output_folder + "/batches/iter_{}.tar.gz".format(iter), batch_folder + "/iter_{}.tar.gz".format(iter))

if __name__ == "__main__":

	parser = argparse.ArgumentParser(description="Running the software in batches. Data must be prepared BEFORE running this, so that the data is already encoded and DB is ready")
	parser.add_argument("--threads", help="Number of threads to use", default=1, type=int)
	parser.add_argument("--output_folder", help="A folder where all data will be stored in the end.", required=True)
	parser.add_argument("--batch_folder", help="Where to copy all the done batches", required=True)
	parser.add_argument("--local_folder", help="Local folder where the data will be copied, if not present, no copying will be done")
	parser.add_argument("--e_value", help="E-value for BLAST. Lowering this will increase computing time, but will also find more hits and shorter", default=0.001)
	parser.add_argument("--word_size", help="Word size for BLAST. Basically character n-gram size.", default=6)
	parser.add_argument("--iter", help="Current iteration", type=int, required=True)
	parser.add_argument("--text_count", help="Text count", type=int, required=True)
	parser.add_argument("--qpi", help="Queries per iteration", type=int, required=True)
	parser.add_argument("--preset", help="Some presets for certain systems.")

	args = parser.parse_args()

	if "preset" in args:
		if args.preset == "taito":
			print("Using preset: Taito")
			args.local_folder = os.environ.get("TMPDIR") + "/" + args.output_folder.split("/")[-1]

	if "local_folder" in args:
		copy_output_folder_to_local(args.output_folder, args.local_folder)
		runner = MultipleBlastRunner(output_folder=args.local_folder, e_value=args.e_value, word_size=args.word_size, threads=args.threads, iter=args.iter, queries_per_iter=args.qpi, text_count=args.text_count)
		runner.run()
		copy_local_data_back(args.local_folder, args.batch_folder, args.iter)
		#delete_local_data(args.local_folder)
	else:
		runner = MultipleBlastRunner(output_folder=args.output_folder, e_value=args.e_value, word_size=args.word_size, threads=args.threads, iter=args.iter, queries_per_iter=args.qpi, text_count=args.text_count)
		runner.run()
		copy_local_data_back(args.output_folder, args.batch_folder, args.iter)
