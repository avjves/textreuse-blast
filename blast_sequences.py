import argparse, os
from blastseq import BlastSeq

if __name__ == "__main__":

	parser = argparse.ArgumentParser(description="Using BLASTP to find repetitions in texts.")
	parser.add_argument("-b", "--blast_location", help="BLASTP location. If not set, using global installation", default="blastp")
	parser.add_argument("-t", "--temp_location", help="Folder to stash data and save results", required=True)
	parser.add_argument("-q", "--query_location", help="Location to a query file. Query file needs to be a FASTA or multi-FASTA file.", required=True)
	parser.add_argument("-m", "--min_length", help="Minimum length for repetitions.", default=100)
	parser.add_argument("-d", "--database_location", help="Location to the database to query against", required=True)

	args = parser.parse_args()
	if not os.path.exists(args.temp_location):
		os.makedirs(args.temp_location)

	if not args.temp_location.endswith("/"):
		args.temp_location += "/"

	seq = BlastSeq(args.temp_location, args.blast_location, args.database_location, args.query_location, args.min_length)
	seq.run()
