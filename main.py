import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.typehints.typehints import Any, Dict, List, Tuple, KV, Iterable
import re

pipeline_options = PipelineOptions()
pipeline = beam.Pipeline(options=pipeline_options)

colunas = [
    "id",
    "data_iniSE",
    "casos",
    "ibge_code",
    "cidade",
    "uf",
    "cep",
    "latitude",
    "longitude",
]

# Pipeline de dengue
def texto_para_lista(elemento: str, delimitador: str = "|") -> List[str]:

    """Recebe um elemento e um delimitador e retorna uma lista
    feita dividindo o elemento pelo delimitador

    Args:
        elemento str: O elemento lido
        delimitador str: O delimitador pelo qual o elemento será dividido
    Returns:
        List[str]: A lista com o conteúdo do elemento
        divido pelo delimitador
    """

    return elemento.split(delimitador)


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


def trata_data(elemento: Dict[str, Any]) -> Dict[str, Any]:
    """
    Resumo
    ------
    Recebe um dicionário e cria uma coluna com a chave 'ano_mes'
    com o ano e mes do campo 'data_iniSE'
    """
    elemento["ano_mes"] = "-".join(elemento["data_iniSE"].split("-")[:2])
    return elemento


def chave_uf(elemento: Dict[str, Any]) -> KV[str, Any]:
    """
    Resumo
    ------
    Recebe um dicionário e retorna uma tupla com o estado(UF) e o dicionário
    (UF, dicionário)
    """
    chave = elemento["uf"]
    return (chave, elemento)


def casos_dengue(elemento: KV[str, Any]) -> Iterable[KV[str, float]]:
    """
    Resumo
    ------
    Recebe uma tupla com a chave UF e o dicionario do elemento
    e retorna uma tupla com a chave UF-ano_mes e o numero de casos
    """

    uf, registros = elemento
    for r in registros:
        casos = float(r["casos"]) if bool(re.search(r"\d", r["casos"])) else 0
        yield (f'{uf}-{r["ano_mes"]}', casos)


# Pipeline de chuvas
def chave_uf_ano_mes(elemento: List[str]) -> KV[str, float]:
    """Resumo

    Args:
        elemento (List[str]): Uma lista com os elementos lidos

    Returns:
        Tuple[str, str]: Uma tupla da chave composta de UF-ANO-MES
        e o valor da quantidade de chuvas em mm

        Exemplo:
        (SC-2022-05, 500.0)
    """
    data, mm, uf = elemento
    ano_mes = data[:7]
    chave = f"{uf}-{ano_mes}"
    mm = max(float(mm), 0.0)
    return chave, mm


def arredonda(elemento: KV[str, float]) -> KV[str, float]:
    """Recebe uma tupla de (chave, valor) e retorna
    a tupla com valor arredondado a uma casa decimal

    Args:
        elemento (Tuple[str, float]): A tupla do elemento no formato
        (chave, valor)

    Returns:
        Tuple[str, float]: A tupla do elemento no formato
        (chave, valor) com valor arredondado
    """
    chave, soma = elemento
    return chave, round(soma, 1)


# Pipeline de união
def filtra_campos_vazios(elemento: KV[str, Any]) -> bool:
    """Remove elementos com chaves vazias

    Args:
        elemento (Tuple[str, dict]): A tupla do elemento
        no formato (chave, valores). e.g.
        ('CE-2014-12', {'chuvas': [], 'dengue': [65.0]})

    Returns:
        bool: Verdadeiro se todos os campos em
        valores estiverem preenchidos
    """
    chave, valores = elemento
    return all(valores.values())


def descompactar_elementos(elemento: KV[str, Any]) -> Tuple[str, str, str, str, str]:
    """Descompacta a chave e o dicionário da tupla

    Args:
        elemento (KV[str, Any]): O elemento no formato
        ('CE-2014-12', {'chuvas': [10.0], 'dengue': [65.0]})

    Returns:
        Tuple[str, str, str, str, str]: Uma tupla descompactada do elemento
        no formato ('CE','2014','12', '10.0', '65.0')
    """

    chave, valores = elemento
    uf, ano, mes = chave.split("-")
    chuvas, dengue = valores["chuvas"][0], valores["dengue"][0]
    return (uf, ano, mes, str(chuvas), str(dengue))


def preparar_csv(elemento: Tuple[str, str, str, str, str], delimitador=";") -> str:
    """Recebe uma tupla com o conteudo da linha
    e retorna uma string com os valores separados por um delimitador

    Args:
        elemento (Tuple[str, str, str, str, str]): A tupla a ser transformada
        delimitador str: O delimitador que será utilizado na construção da linha
    Returns:
        str: Uma linha com o conteudo da tupla separado pelo delimitador
    """
    return delimitador.join(elemento)


dengue = (
    pipeline
    | "Leitura do dataset de dengue"
    >> ReadFromText(
        "./alura-apachebeam-basedados/casos_dengue_sample.txt", skip_header_lines=1
    )
    | "Convertendo texto para lista" >> beam.Map(texto_para_lista)
    | "Convertendo lista para dicionario" >> beam.Map(lista_para_dict, colunas)
    | "Criando a coluna ano_mes" >> beam.Map(trata_data)
    | "Criar a chave pelo estado" >> beam.Map(chave_uf)
    | "Agrupando por estado" >> beam.GroupByKey()
    | "Descompactar casos de dengue" >> beam.FlatMap(casos_dengue)
    | "Somar os casos de dengue para cada estado-mes" >> beam.CombinePerKey(sum)
    # | "Mostrando resultados dengue" >> beam.Map(print)
)

chuvas = (
    pipeline
    | "Leitura do dataset de chuvas"
    >> ReadFromText(
        "./alura-apachebeam-basedados/chuvas_sample.csv", skip_header_lines=1
    )
    | "Convertendo texto para lista (chuvas)"
    >> beam.Map(texto_para_lista, delimitador=",")
    | "Criando chave pelo estado e ano_mes" >> beam.Map(chave_uf_ano_mes)
    | "Soma por chave ano mês" >> beam.CombinePerKey(sum)
    | "Arredondando valores para 2 casas decimais" >> beam.Map(arredonda)
    # | "Mostrando resultados chuvas" >> beam.Map(print)
)

resultado = (
    # (chuvas, dengue)
    # | "Unindo o resultado das pipelines" >> beam.Flatten()
    # | "Agrupando dados de chuvas e dengue por chave" >> beam.GroupByKey()
    ({"chuvas": chuvas, "dengue": dengue})
    | "Mesclando pcolletions" >> beam.CoGroupByKey()
    | "Filtrar dados vazios" >> beam.Filter(filtra_campos_vazios)
    | "Descompactando dados" >> beam.Map(descompactar_elementos)
    | "Preparar CSV" >> beam.Map(preparar_csv)
    # | "Mostrando resultado de ambas" >> beam.Map(print)
)

header = "uf,ano,mes,chuvas,dengue"
resultado | "Criando CSV" >> WriteToText(
    "resultado", file_name_suffix=".csv", header=header
)

pipeline.run()
