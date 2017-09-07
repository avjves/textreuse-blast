import re

class TextEncoder:
	
	def __init__(self, language):
		self.mapping = self.make_mapping(language)
		
	
	def make_mapping(self, language):
		if language.lower() == "fin":
			return {"ä": "W", "v": "V", "a": "B", "i": "C", "s": "H", "m": "N", "d": "Z", "h": "S", "e": "G", " ": "A", "u": "K", "f": "W", "w": "Y", "o": "I", "y": "T", "l": "F", "j": "R", "n": "E", "r": "P", "k": "M", "p": "Q", "t": "D", "z": "X"}


	def preprocess_text(self, text):
		return " ".join(text.lower().split())
		
	## Encodes text into a protein sequence
	def encode_text(self, text):
		encoded_text = self.preprocess_text(text)
		encoded_text = re.sub("[^" + "".join(list(self.mapping.keys())) + "]", "", encoded_text, flags=re.DOTALL)
		for char, amino in self.mapping.items():
			encoded_text = re.sub(u"["+char+"]", amino, encoded_text, flags=re.DOTALL)
		return encoded_text
	
	## Masks all character to X that don't have an equivalent in the mapping
	def encode_mask(self, text):
		return re.sub("[^" + "".join(list(self.mapping.keys())) + "]", "X", text, flags=re.DOTALL)
	
	## Calculate actual text
	def decode_text(self, text, start_index, end_index):
		start_index, end_index = int(start_index)-1, int(end_index)-1
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
		

