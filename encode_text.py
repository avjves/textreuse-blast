import argsparse, json, codecs
from text_encoder import TextEncoder

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Encode text to protein sequences.")
	parser.add_argument("-l", "--language", help="Text language. Only 22 letters available, so depending on the language some letters are ignored. Currently available are FIN, SWE and ENG. Default: ENG", default="ENG")
	parser.add_argument("-t", "--text", help="Text location. Text is to be in JSON-format, key = text id, value = text itself", required=True)
	parser.add_argument("-o", "--output", help="Where to save the file. If not set, stdout", default="")

	args = parser.parse_args()

	if args.language not in ["FIN", "SWE", "ENG"]:
		raise ValueError("Language unsupported!")

	encoder = TextEncoder(args.language)

	with codecs.open(args.text, "r") as json_file:
		jdata = json.load(json_file)

	for key, value in jdata.items():
		jdata[key] = encoder.encode_text_22(value)

	if args.output == "":
		sys.stdout.write(json.dumps(jdata))
		sys.stdout.flush()
	else:
		with codecs.open(args.output, "w") as output_file:
			json.dump(jdata, output_file)

	

