import re

class TextEncoder:

	def __init__(self, language):
		self.mapping = self.make_mapping(language)


	def make_mapping(self, language):
		if language.lower() == "fin":
			return {"ä": "W", "v": "V", "a": "B", "i": "C", "s": "H", "m": "N", "d": "Z", "h": "S", "e": "G", "u": "K", "f": "W", "w": "Y", "o": "I", "y": "T", "l": "F", "j": "R", "n": "E", "r": "P", "k": "M", "p": "Q", "t": "D", "z": "X"}

		elif language.lower() == "eng":
			return {"u": "E", "n": "W", "t": "R", "b": "V", "d": "C", "g": "Z", "f": "N", "s": "F", "m": "K", "w": "M", "i": "G", "p": "I", "o": "Y", "e": "H", "h": "S", "r": "T", "l": "A", "k": "X", "c": "B", "a": "W", "y": "Q", "j": "D", "v": "P"}

		elif language.lower() == "eng_space":
			return {"o": "Y", "a": "W", "t": "R", "f": "N", "w": "M", "h": "S", "l": "A", "v": "P", "y": "Q", " ": "D", "e": "H", "r": "T", "i": "G", "s": "F", "p": "I", "c": "B", "b": "V", "m": "K", "d": "C", "g": "Z", "u": "E", ".": "X", "n": "W"}


	''' Preprocess text by removing extra whitespaces '''
	def preprocess_text(self, text):
		processed_text = " ".join(text.lower().split())
		return processed_text

	'''Encodes text into a protein sequence '''
	def encode_text(self, text, preprocess=True):
		if preprocess:
			encoded_text = self.preprocess_text(text)
		else:
			encoded_text = text.lower()
		encoded_text = re.sub("[^" + "".join(list(self.mapping.keys())) + "]", "", encoded_text, flags=re.DOTALL)
		for char, amino in self.mapping.items():
			encoded_text = re.sub(u"["+char+"]", amino, encoded_text, flags=re.DOTALL)
		return encoded_text

	''' Masks all character to X that don't have an equivalent in the mapping '''
	def encode_mask(self, text):
		return re.sub("[^" + "".join(list(self.mapping.keys())) + "]", "X", text, flags=re.DOTALL)

	''' reverse protein mapping, not fully decoded, just using the reverse map here '''
	def decode_enc_text(self, text):
		for char, amino in self.mapping.items():
			text = re.sub(u"[" + amino + "]", char, text, flags=re.DOTALL)
		return text


	''' Calculate back the actual text from the protein sequence indexes '''
	def decode_text(self, text, start_index, end_index, preprocess=True):
		start_index, end_index = int(start_index), int(end_index)-1
		if preprocess:
			text = self.preprocess_text(text)
		masked = self.encode_mask(text)
		enc_index = 0
		indices = []
		for index, letter in enumerate(masked):
			if enc_index == start_index and len(indices) == 0:
				indices.append(index)
			elif enc_index == end_index and len(indices) == 1:
				indices.append(index)
				break
			if letter != "X":
				enc_index += 1
		return text[indices[0]:indices[1]], indices
