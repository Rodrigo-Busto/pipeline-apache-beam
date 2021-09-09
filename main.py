import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.typehints.typehints import Any, Dict, List

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

dengue = (
    pipeline
    | "Leitura do dataset de dengue" >> 
        ReadFromText("./alura-apachebeam-basedados/casos_dengue.txt", skip_header_lines=1)
    | "Convertendo texto para lista" >> beam.Map(texto_para_lista)
    | "Convertendo lista para dicionario" >> beam.Map(lista_para_dict, colunas)
    | "Criando a coluna ano_mes" >> beam.Map(trata_data)
    | "Criar a chave pelo estado" >> beam.Map(chave_uf)
    | "Agrupando por estado" >> beam.GroupByKey()
    | "Mostrando resultados" >> beam.Map(print)
)

pipeline.run()