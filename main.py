from typing import Iterable
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(argv = None)
pipeline = beam.Pipeline(options = pipeline_options)

def lista_para_dict(elemento: Iterable[str], colunas: Iterable[str]) -> dict:
    
    """
    Resumo
    ------
    Converte uma lista para um dicionário dadas as chaves

    Returns:
    -------
    Retorna um dicionário dos elementos
    """
    
    return dict(zip(colunas, elemento))

def texto_para_lista(elemento: str, delimitador = "|") -> list:
    
    """
    Resumo
    -------
    Recebe um texto e um delimitador

    Retorna
    -------
        uma lista de textos a partir 
        do original quebrado pelo delimitador
    """
    
    return elemento.split(delimitador)

dengue = (
    pipeline
    | "Leitura do dataset de dengue" >> 
        ReadFromText("./alura-apachebeam-basedados/casos_dengue.txt", skip_header_lines=1)
    | "Convertendo texto para lista" >> beam.Map(texto_para_lista)
    | "Convertendo lista para dicionario" >> beam.Map(lista_para_dict)
    | "Mostrando resultados" >> beam.Map(print)
)

pipeline.run()