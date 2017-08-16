from __future__ import print_function
import json
import sys
import re
import codecs
import gzip

class TextEncoder(object):

	def __init__(self, lang="ENG"):
		if lang == "FIN":
			dictionary = {"ä": "W", "v": "V", "a": "B", "i": "C", "s": "H", "m": "N", "d": "Z", "h": "S", "e": "G", " ": "A", "u": "K", "f": "W", "w": "Y", "o": "I", "y": "T", "l": "F", "j": "R", "n": "E", "r": "P", "k": "M", "p": "Q", "t": "D", "z": "X"}
		elif lang == "SWE":
			dictionary = {"ä": "T", "h": "R", "a": "C", "i": "G", "s": "I", "c": "V", "m": "N", "d": "K", "b": "W", "e": "B", " ": "A", "u": "S", "f": "M", "g": "P", "o": "W", "r": "F", "l": "H", "n": "D", "ö": "Y", "k": "Q", "p": "Z", "t": "E"}
		else:
			dictionary = {"o": "Y", "a": "W", "t": "R", "f": "N", "w": "M", "h": "S", "l": "A", "v": "P", "y": "Q", " ": "D", "e": "H", "r": "T", "i": "G", "s": "F", "p": "I", "c": "B", "b": "V", "m": "K", "d": "C", "g": "Z", "u": "E", ".": "X", "n": "W"}

		self.alphabet_22 = dictionary
		self.keys = u""
		for key, value in iter(self.alphabet_22.items()):
			self.keys += key
		self.inverse_alphabet_22 = {v:k for k, v in iter(self.alphabet_22.items())}
		self.keys = self.keys.replace(" ", "")


	def encode_text_22(self, text):
		encoded_text = " ".join(text.lower().split())
		encoded_text = re.sub(u"[^"+self.keys+"]", "", encoded_text, flags=re.DOTALL)
		for key, value in iter(self.alphabet_22.items()):
			encoded_text = re.sub(u"["+key+"]", value, encoded_text, flags=re.DOTALL)

		return encoded_text


	def encode_text(self, text):
		encoded_text = " ".join(text.lower().split())
		encoded_text = re.sub(u"[^"+self.keys+"]", "", encoded_text, flags=re.DOTALL)
		for key, value in self.alphabet_22.items():
			encoded_text = re.sub(u"[" + key + "]", value, encoded_text, flags=re.DOTALL)

		return encoded_text


	def encode_word(self, word, case):
		encoded = []
		for letter in word:
			encoded.append(self.alphabet_22.get(letter, ""))
		if case == "upper":
			return "".join(encoded)
		else:
			return "".join(lower)

	def decode_text_22(self, text):
		decoded_text = text
		for key, value in iter(self.inverse_alphabet_22.items()):
			decoded_text = re.sub(u"[" + key + "]", value, decoded_text, flags=re.DOTALL)
		return decoded_text


	def encode_text_X_prot(self, text):
		encoded_text = text.lower()
		encoded_text = re.sub(u"[^"+self.keys+"]", "X", encoded_text, flags=re.DOTALL)
		return encoded_text


	## Requires the original text and the indexes from BLAST
	def get_original_text(self, original_full_text, i_start, i_end):
		i_start = int(i_start) - 1
		if i_start == -1:
			i_start = 0
		i_end = int(i_end) - 1
		original_full_text = " ".join(original_full_text.split())

		x_text = self.encode_text_X_prot(original_full_text)
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
		return original_full_text[indices[0]:indices[1]]

	def get_original_text_and_indices(self, original_full_text, i_start, i_end):
		i_start = int(i_start) - 1
		if i_start == -1:
			i_start = 0
		i_end = int(i_end) - 2
		original_full_text = " ".join(original_full_text.split())

		x_text = self.encode_text_X_prot(original_full_text)
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
		return original_full_text[indices[0]:indices[1]], indices[0], indices[1]

	def get_original_text_indexes(self, original_full_text, i_start, i_end):
			i_start = int(i_start)-1
			if i_start == -1:
				i_start = 0
			i_end = int(i_end)-1
			original_full_text = " ".join(original_full_text.split())

			x_text = self.encode_text_X_prot(original_full_text)
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
			#try:
				#return original_full_text[indices[0]:indices[1]]
			return [indices[0], indices[1]]
			#except IndexError:
			#	return [indices[0], None]
