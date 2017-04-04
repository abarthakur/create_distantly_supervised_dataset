"""
Add DBpedia spotlight annotations on top of Infolink anotations.
"""
from functools import partial
import json
import sys
from spotlight import annotate

def spot_entities(text, api):
    """
    Query dbpedia.
    """
    results = api(text)
    entities = []
    for result in results:
        start = result['offset']
        name = result.get('surfaceForm', 0)
        types = result['types'].split(',')
        labels = [x[len('DBpedia:'):] for x in types if 'DBpedia:' in x]
        if not labels:
            labels.append('None')
        # some time surface form is missing from result
        # ignore them
        if name:
            if not isinstance(name, str):
                name = str(name)
            end = start + len(name)
            entities.append({
                'start' : start,
                'name' : name,
                'end' : end,
                'labels' : labels
                })
    return entities

#pylint:disable=invalid-name
def process_file(input_file_path, spotlight_api, output_file_path):
    """
    Add layer of Dbpedia spotlight annotation on input file.
    """

    wiki_file = open(input_file_path, "r", encoding='utf-8')
    output_file = open(output_file_path, 'w', encoding='utf-8')
    for line in wiki_file:
        json_data = json.loads(line)
        text = json_data['text']
        infoEMs = json_data['infoEMs']
        pid = json_data['pid']
        fid = json_data['fid']
        spoted_entities = spot_entities(text, spotlight_api)
        output_file.write(
            json.dumps({
                'text': text,
                'infoEMs': infoEMs,
                'spotEMs': spoted_entities,
                'pid': pid,
                'fid': fid
            }) + '\n'
        )
    wiki_file.close()
    output_file.close()

if __name__ == "__main__":
    process_file(sys.argv[1],
                 partial(annotate,
                         'http://localhost:9999/rest/annotate',
                         confidence=0.3,
                         support=1),
                 sys.argv[2])
