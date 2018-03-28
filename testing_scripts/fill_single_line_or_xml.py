import argparse, lmdb, sys, os
sys.path.insert(0, "/".join(os.path.abspath(__file__).split("/")[:-2]))
from text_encoder import TextEncoder



def fill_line(from_id, line, output_location, lang):
    splits = line.split()
    curr_id = splits[0].replace("\\", "")
    print(curr_id)
    orig_text_from = lmdb.open(output_location + "db/original_data_DB",readonly=True).begin().get(from_id.encode("ascii")).decode("unicode-escape")
    orig_text_curr = lmdb.open(output_location + "db/original_data_DB",readonly=True).begin().get(curr_id.encode("ascii")).decode("unicode-escape")

    #print(orig_text)
    indexes = splits[3:5]
    print(indexes)
    encoder = TextEncoder(lang)
    from_enc = encoder.decode_text(orig_text_from, splits[1], splits[2])
    curr_enc = encoder.decode_text(orig_text_curr, splits[3], splits[4])

    return from_enc, curr_enc



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fill single line or a file")
    parser.add_argument("--output_folder", help="A folder where all data is stored (Like DBs)", required=True)
    parser.add_argument("--language", help="Encoding language", default="FIN")
    parser.add_argument("--file_location")
    parser.add_argument("--from_id", default="")
    parser.add_argument("--line", default="")

    args = parser.parse_args()
    if args.line and args.from_id:
        print(fill_line(args.from_id, args.line, args.output_folder, args.language))
