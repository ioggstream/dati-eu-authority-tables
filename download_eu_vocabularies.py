import json
import logging
from multiprocessing import Pool
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

import click
import pandas as pd
import pytest
import requests
import yaml
from frictionless import Resource
from pyld import jsonld
from rdflib.graph import Graph
from rdflib.plugins.serializers.jsonld import from_rdf
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
    data = requests.get(f"{sparql_endpoint}?" + ep, timeout=1)
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

            yield download_url, dest_file
        except (KeyError, IndexError):
            print(f"Cannot parse {download_url}")


def download_file(url, dest_file):
    if dest_file.exists():
        return

    g = Graph()
    data = client.get(url, timeout=1)
    if data.status_code != 200:
        log.error(f"Erro retrieving {url}: {data.status_code}")
        return

    dest_file.parent.mkdir(exist_ok=True, parents=True)
    file_rdf = dest_file.with_suffix(".rdf")
    file_rdf.write_bytes(data.content)
    g.parse(file_rdf.as_posix(), format="application/rdf+xml")
    g.serialize(format="text/turtle", destination=dest_file.as_posix())
    log.warning("Returning %s %s", url, dest_file)
    return (url, dest_file)


def to_jsonld(url, src_file):
    dest_file = src_file.with_suffix(".jsonld")
    g = Graph()
    g.parse(src_file.as_posix())
    g.serialize(format="json-ld", destination=dest_file.as_posix())
    return url, dest_file


def to_json(url, src_file):
    g = Graph()
    g.parse(src_file.with_suffix(".ttl").as_posix())
    data = from_rdf(g)
    frame = yaml.safe_load(Path("frame-short.yamlld").read_text())
    frame.pop("_meta")
    json_data = jsonld.frame(data, frame)
    context, graph = json_data["@context"], json_data["@graph"]
    dest_json = src_file.with_suffix(".json")
    dest_json.write_text(json.dumps(graph, indent=2))

    write_datapackage(graph, context, src_file)

    return url, dest_json


def write_datapackage(graph, context, dest_file):

    resource = Resource(data=graph)
    resource.infer()
    resource_dict = resource.to_dict()
    resource_dict.update(
        {"name": dest_file.stem, "path": dest_file.with_suffix(".csv").name}
    )
    resource_dict.pop("data")
    datapackage_yaml = Path(dest_file.parent / "datapackage.yaml")
    if datapackage_yaml.exists():
        datapackage = yaml.safe_load(datapackage_yaml.read_text())
        for r in datapackage["resources"]:
            if r["name"] == dest_file.stem:
                r.update(resource_dict)
                break
        else:
            datapackage["resources"].append(resource_dict)
    else:
        datapackage = {
            "name": dest_file.stem,
            "resources": [resource_dict],
        }
    datapackage["resources"][-1]["schema"]["x-jsonld-context"] = context
    if gtype := graph[0].get("@type"):
        datapackage["resources"][-1]["schema"]["x-jsonld-type"] = gtype
    datapackage_yaml.write_text(yaml.safe_dump(datapackage))


def to_csv(url, src_file):
    out = json.loads(src_file.read_text())
    dest_csv = src_file.with_suffix(".csv")

    df = pd.json_normalize(out, meta=[])
    df.drop(columns=["@type"], inplace=True)
    df.to_csv(dest_csv, index=False, sep=";", header=True, quotechar='"')
    return url, src_file.with_suffix(".csv")


def pipeline(*args):
    for f in (
        download_file,
        to_json,
        to_csv,
    ):
        log.warning("Running %s(%s)", f.__name__, args)
        args = f(*args)
        if args is None:
            return
        log.warning("Result %s", args)


@click.command()
@click.option("--forks", default=1, help="processes to spawn")
@click.option("--needle", default="", help="tables to get")
def main(forks, needle):
    vocabularies = get_vocabularies(URL)

    with Pool(processes=forks) as workers:
        workers.starmap(pipeline, ((x, y) for x, y in vocabularies if needle in x))


def test_humansexes():
    dest_file = next(Path("./assets/vocabularies/").glob("**/humansexes.ttl"))
    _, dest_file = to_json(None, dest_file)
    to_csv(None, dest_file)
    assert (dest_file.parent / "datapackage.yaml").exists()


@pytest.mark.parametrize("fpath", Path("assets/").glob("**/*.ttl"))
def test_csv(fpath):
    _, dest_file = to_json(None, fpath)
    assert dest_file.exists()
    _, dest_file = to_csv(None, dest_file)
    assert dest_file.exists()


# pylint: disable=no-value-for-parameter
if __name__ == "__main__":
    main()
