# absa_quad_extractor/run_pipeline.py

from ast import Assign
from . import config
from .data_loader import DataLoader
from .extract import QuadrupleExtractor
from .processor import extract_quadruples
from .saver import save_to_csv

class PipelineRunner:
    def __init__(self, input_path=config.INPUT_CSV, output_path=config.OUTPUT_CSV, model_name=config.MODEL_NAME):
        self.input_path = input_path
        self.output_path = output_path
        self.model_name = model_name
        self.df = None
        self.reviews = None
        self.model = None

    design = 50
    def load_data(self):
        print("=="*self.design)
        print("🚀 Loading data...")
        print("=="*self.design)
        self.df, self.reviews = DataLoader.load_data(self.input_path)

    def load_model(self):
        print("=="*self.design)
        print("🤖 Loading model...")
        print("=="*self.design)
        self.model = QuadrupleExtractor(model_name=self.model_name)

    def extract(self):
        print("=="*self.design)
        print("🔍 Extracting quadruples...")
        print("=="*self.design)
        aspects, opinions, sentiments, categories = extract_quadruples(self.reviews, self.model)
        print("=="*self.design)
        print("🧩 Adding columns...")
        print("=="*self.design)
        self.df["Aspects"] = aspects
        self.df["Opinions"] = opinions
        self.df["Sentiments"] = sentiments
        self.df["Categories"] = categories

    def save(self):
        print("=="*self.design)
        print("💾 Saving results...")
        print("=="*self.design)
        save_to_csv(self.df, self.output_path)

    def run(self):
        self.load_data()
        self.load_model()
        self.extract()
        self.save()

if __name__ == "__main__":
    runner = PipelineRunner()
    runner.run()
