import sys

import json
from api import get_results
import pandas as pd
from multiprocessing import Pool
from tqdm import tqdm
import glob
import os


def create_directory_if_not_exists(dir_path):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)


endpoint_url = "https://query.wikidata.org/sparql"


def get_metadata(wiki_id):
    query = """SELECT  ?subject ?subjectLabel
        ?instance ?instanceLabel
        ?subclass ?subclassLabel
        ?inception
        ?time_period ?time_periodLabel
        ?culture ?cultureLabel 
        ?architecture_style ?architecture_styleLabel 
        ?founded_by ?founded_byLabel 
        ?creator ?creatorLabel
        ?country ?countryLabel
        ?territory ?territoryLabel
        ?genre ?genreLabel
        ?movement ?movementLabel
        ?author ?authorLabel
        ?publicationdate
        ?country_origin ?country_originLabel
        ?wikipediaLink
WHERE {
  BIND(wd:%s AS ?subject)
  OPTIONAL { ?subject wdt:P31 ?instance. }
  OPTIONAL { ?subject wdt:P50 ?author. }
  OPTIONAL { ?subject wdt:P279 ?subclass. }
  OPTIONAL { ?subject wdt:P571 ?inception. }
  OPTIONAL { ?subject wdt:P2348 ?time_period. }
  OPTIONAL { ?subject wdt:P2596 ?culture. }
  OPTIONAL { ?subject wdt:P149 ?architecture_style. }
  OPTIONAL { ?subject wdt:P112 ?founded_by. }
  OPTIONAL { ?subject wdt:P170 ?creator. }
  OPTIONAL { ?subject wdt:P17 ?country. }
  OPTIONAL { ?subject wdt:P131 ?territory. }
  OPTIONAL { ?subject wdt:P136 ?genre. }
  OPTIONAL { ?subject wdt:P135 ?movement. }
  OPTIONAL { ?subject wdt:P577 ?publicationdate. }
  OPTIONAL { ?subject wdt:P495 ?country_origin. }
  
  OPTIONAL {
    ?wikipediaLink schema:about ?subject;
                  schema:isPartOf <https://en.wikipedia.org/>.
  }
  
  SERVICE wikibase:label {
    bd:serviceParam wikibase:language "en".
  }
}


    """ % (
        wiki_id
    )

    return query


def final_function(wiki_id):
    try:
        results = get_results(endpoint_url, query=get_metadata(wiki_id=wiki_id))
        results = results["results"]["bindings"]
        return results
    except:
        return None


def process_chunk(chunk, chunk_index):
    with Pool(8) as p:
        results = list(tqdm(p.imap(final_function, chunk), total=len(chunk)))
    with open(f"raw_data/extracts/results_{chunk_index}.json", "w") as f:
        json.dump(results, f)


def main():
    data = pd.read_csv("raw_data/literay_works.csv")
    data["wikidata_id"] = data["work"].apply(
        lambda x: x.split("http://www.wikidata.org/entity/")[1]
    )

    wikis = list(data["wikidata_id"])
    chunk_size = 10000

    for i in range(0, len(wikis), chunk_size):
        chunk = wikis[i : i + chunk_size]
        chunk_index = i // chunk_size
        process_chunk(chunk, chunk_index)


if __name__ == "__main__":
    main()


# if __name__ == "__main__":

#     import pandas as pd

#     data = pd.read_csv("raw_data/literay_works.csv")
#     data["wikidata_id"] = data["work"].apply(
#         lambda x: x.split("http://www.wikidata.org/entity/")[1]
#     )

#     wikis = list(data["wikidata_id"])

#     final_list = []

#     # for wiki in tqdm(wikis, total=len(wikis)):
#     #     result = final_function(wiki)
#     #     final_list.append(result)

#     with Pool(8) as p:
#         results = list(tqdm(p.imap(final_function, wikis), total=len(wikis)))

#     # results_df = pd.DataFrame(results)

#     with open("raw_data/results.json", "w") as f:
#         json.dump(results, f)
