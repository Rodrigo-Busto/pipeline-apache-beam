import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.typehints.typehints import Any, Dict, List
import re

pipeline_options = PipelineOptions(argv = None)
pipeline = beam.Pipeline(options = pipeline_options)

colunas = [
    "id", 
    "data_iniSE", 
    "casos", 
    "ibge_code", 
    "cidade", 
    "uf", 
    "cep", 
    "latitude", 
    "longitude"
    ]

def lista_para_dict(elemento: List[str], colunas: List[str]) -> Dict[str, Any]:
    
    """
    Resumo
    ------
    Converte uma lista para um dicionário dadas as chaves

    Returns:
    -------
    Retorna um dicionário dos elementos
    """
    
    return dict(zip(colunas, elemento))

def texto_para_lista(elemento: List[str], delimitador: str = "|") -> List[str]:
    
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

def trata_data(elemento: Dict[str, Any]):
    """
    Resumo
    ------
    Recebe um dicionário e cria uma coluna com a chave 'ano_mes'
    com o ano e mes do campo 'data_iniSE'
    """
    elemento['ano_mes'] = '-'.join(elemento['data_iniSE'].split('-')[:2])
    return elemento

def chave_uf(elemento):
    """
    Resumo
    ------
    Recebe um dicionário e retorna uma tupla com o estado(UF) e o dicionário
    (UF, dicionário)
    """
    chave = elemento['uf']
    return (chave, elemento)

def casos_dengue(elemento):
    """
    Resumo
    ------
    Recebe uma tupla com a chave UF e o dicionario do elemento
    e retorna uma tupla com a chave UF-ano_mes e o numero de casos
    """
    
    uf, registros = elemento
    for r in registros:
        casos = float(r['casos']) if bool(re.search(r"\d", r['casos'])) else 0
        yield (f'{uf}-{r["ano_mes"]}', casos)

dengue = (
    pipeline
    | "Leitura do dataset de dengue" >> 
        ReadFromText("./alura-apachebeam-basedados/casos_dengue.txt", skip_header_lines=1)
    | "Convertendo texto para lista" >> beam.Map(texto_para_lista)
    | "Convertendo lista para dicionario" >> beam.Map(lista_para_dict, colunas)
    | "Criando a coluna ano_mes" >> beam.Map(trata_data)
    | "Criar a chave pelo estado" >> beam.Map(chave_uf)
    | "Agrupando por estado" >> beam.GroupByKey()
    | "Descompactar casos de dengue" >> beam.FlatMap(casos_dengue)
    | "Somar os casos de dengue para cada estado-mes" >> beam.CombinePerKey(sum)
    | "Mostrando resultados" >> beam.Map(print)
)

pipeline.run()