import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(argv = None)
pipeline = beam.Pipeline(options = pipeline_options)

dengue = (
    pipeline
    | "Leitura do dataset de dengue" >> 
        ReadFromText("./alura-apachebeam-basedados/casos_dengue.txt", skip_header_lines=1)
)