# absa_quad_extractor/extractor.py
from pyabsa import ABSAInstruction

class QuadrupleExtractor:
    def __init__(self, model_name="multilingual"):
        self.extractor = ABSAInstruction.ABSAGenerator(model_name)

    def predict(self, text, max_length=512):
        return self.extractor.predict(text, max_length=max_length)
