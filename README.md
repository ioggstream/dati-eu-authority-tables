# dati-eu-authority-tables
An unofficial mirror of EU authority tables.

This repository harvests on daily basis the EU authority tables
and generates CSV frictionless datapackages out of them.

## How it works

There's a daily cronjob in a github action:

1. Get the authority tables from the EU SparQL endpoint;
2. Harvest them retrieving the RDF and converting in Turtle;
3. Use JSON-LD framing to project a subset of the information in JSON,
   then use pandas to generate a CSV;
4. Use the provided metadata to add a frictionless datapackage;
5. Commits the retrieved information on the repository.

## Running

To run the harvester locally, just:

        pip install tox
        tox -e run
