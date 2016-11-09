from __future__ import print_function
import json
import sys
import re
import codecs


class TextEncoder(object):

	def __init__(self):
		self.init_new()


	def init_new(self):
		#dictionary_swe = {"ä": "T", "h": "R", "a": "C", "i": "G", "s": "I", "c": "V", "m": "N", "d": "K", "b": "W", "e": "B", " ": "A", "u": "S", "f": "M", "g": "P", "o": "W", "r": "F", "l": "H", "n": "D", "ö": "Y", "k": "Q", "p": "Z", "t": "E"}
		#dictionary_fin = {"ä": "W", "v": "V", "a": "B", "i": "C", "s": "H", "m": "N", "d": "Z", "h": "S", "e": "G", " ": "A", "u": "K", "f": "W", "w": "Y", "o": "I", "y": "T", "l": "F", "j": "R", "n": "E", "r": "P", "k": "M", "p": "Q", "t": "D", "z": "X"}
		dictionary = {"o": "Y", "a": "W", "t": "R", "f": "N", "w": "M", "h": "S", "l": "A", "v": "P", "y": "Q", " ": "D", "e": "H", "r": "T", "i": "G", "s": "F", "p": "I", "c": "B", "b": "V", "m": "K", "d": "C", "g": "Z", "u": "E", ".": "X", "n": "W"}

		self.alphabet_22 = dictionary
		self.keys = u""
		for key, value in iter(self.alphabet_22.items()):
			self.keys += key
		self.inverse_alphabet_22 = {v:k for k, v in iter(self.alphabet_22.items())}


	def encode_text_22(self, text):
		#encoded_text = re.sub("[\\n]", " ", text.lower(), flags=re.DOTALL)
		encoded_text = " ".join(text.lower().split())
		encoded_text = re.sub(u"[^"+self.keys+"]", "", encoded_text, flags=re.DOTALL)
		for key, value in iter(self.alphabet_22.items()):
			encoded_text = re.sub(u"["+key+"]", value, encoded_text, flags=re.DOTALL)

		encoded_text = re.sub(u"D+", "D", encoded_text, flags=re.DOTALL)
		return encoded_text

	def decode_text_22(self, text):
		decoded_text = text
		for key, value in iter(self.inverse_alphabet_22.items()):
			decoded_text = re.sub(u"[" + key + "]", value, decoded_text, flags=re.DOTALL)
		return decoded_text

	def encode_text_X(self, text):
		encoded_text = " ".join(text.lower().split()) ## TODO: All extra characters are to be turned to X
		encoded_text = re.sub(u"[^"+self.keys+"]", "X", encoded_text, flags=re.DOTALL)

		encoded_text = re.sub(u"D+", "D", encoded_text, flags=re.DOTALL) ## First D, rest X
		return encoded_text


	## Requires the original text and the indexes from BLAST
	def get_original_text(self, original_full_text, i_start, i_end):
		i_start = int(i_start) - 1
		i_end = int(i_end) - 1
		x_text = self.encode_text_X(original_full_text)
		encoded_index = 0
		indices = []
		for index, letter in enumerate(x_text):
			if encoded_index == i_start and len(indices) == 0:
				indices.append(index)

			elif encoded_index == i_end and len(indices) == 1:
				indices.append(index)
				break

			if letter != "X":
				encoded_index += 1
		try:
			return original_full_text[indices[0]:indices[1]]
		except IndexError:
			return original_full_text[indices[0]:]


