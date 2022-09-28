import logging
from multiprocessing import Pool
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

import click
import requests
from rdflib.graph import Graph
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

log = logging.getLogger(__name__)


def get_client():
    retry_strategy = Retry(
        total=10,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        method_whitelist=["HEAD", "GET", "OPTIONS"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)
    return http


client = get_client()

QUERY = """
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX odp:  <http://data.europa.eu/euodp/ontologies/ec-odp#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT distinct ?u ?v WHERE {

?s a dcat:Dataset .
?s dcat:distribution ?d .
?s <http://www.w3.org/2002/07/owl#versionInfo> ?v .
?d a dcat:Distribution .
?d dct:format <http://publications.europa.eu/resource/authority/file-type/RDF_XML>  .
  ?d dcat:downloadURL ?u FILTER regex(?u, "-skos.rdf$", "i" )
} LIMIT 100
"""

URL = "https://data.europa.eu/sparql"


def sparql_get(sparql_endpoint, query):
    qp = {
        "query": [query],
        "format": ["application/sparql-results+json"],
        "timeout": ["0"],
        "debug": ["on"],
        "run": [" Run Query "],
    }

    ep = urlencode(qp, doseq=True)
    data = requests.get(f"{sparql_endpoint}?" + ep, timeout=5)
    return data.json()


def get_vocabularies(url):
    ret = sparql_get(url, QUERY)
    for download_url, version in [
        (x["u"]["value"], x["v"]["value"]) for x in ret["results"]["bindings"]
    ]:
        url = urlparse(download_url)
        try:
            file_name = parse_qs(url.query)["fileName"][0]
            dest_file = Path(file_name.replace("-skos", ""))
            dest_file = (
                Path("assets") / "vocabularies" / dest_file.stem / version / dest_file
            ).with_suffix(".ttl")
            dest_file.parent.mkdir(exist_ok=True, parents=True)
            if dest_file.exists():
                continue

            yield download_url, dest_file
        except (KeyError, IndexError):
            print(f"Cannot parse {download_url}")


def download_file(url, dest_file):
    print(url, dest_file)
    g = Graph()
    data = client.get(url, timeout=5)
    if data.status_code != 200:
        log.error(f"Erro retrieving {url}: {data.status_code}")
        return

    dest_file.parent.mkdir(exist_ok=True, parents=True)
    file_rdf = dest_file.with_suffix(".rdf")
    file_rdf.write_bytes(data.content)
    g.parse(file_rdf.as_posix(), format="application/rdf+xml")
    g.serialize(format="text/turtle", destination=dest_file.as_posix())


@click.command()
@click.option("--forks", default=1, help="processes to spawn")
@click.option("--needle", default="", help="tables to get")
def main(forks, needle):
    vocabularies = get_vocabularies(URL)

    with Pool(processes=forks) as workers:
        workers.starmap(download_file, ((x, y) for x, y in vocabularies if needle in x))


# pylint: disable=no-value-for-parameter
if __name__ == "__main__":
    main()
