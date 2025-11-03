import json, argparse
from pathlib import Path
from neo4j import GraphDatabase
from utils_prov import ensure_source_and_activity, link_generated_by_and_derived_from

def upsert_audio(tx, row):
    tx.run("""
        MERGE (a:Audio {uid:$uid})
        ON CREATE SET a.file=$file, a.duration=$dur, a.sr=$sr
        ON MATCH SET a.file=COALESCE(a.file,$file),
                     a.duration=COALESCE(a.duration,$dur),
                     a.sr=COALESCE(a.sr,$sr)
    """, uid=row['uid'], file=row.get('file'), dur=row.get('duration'), sr=row.get('sr'))

    if row.get('scene'):
        tx.run("""
            MERGE (sc:Scene {name:$scene})
            WITH sc
            MATCH (a:Audio {uid:$auid})
            MERGE (a)-[:HAS_SCENE]->(sc)
        """, scene=row['scene'], auid=row['uid'])

    if row.get('city'):
        tx.run("""
            MERGE (c:City {name:$city})
            WITH c
            MATCH (a:Audio {uid:$auid})
            MERGE (a)-[:RECORDED_IN]->(c)
        """, city=row['city'], auid=row['uid'])

    if row.get('device'):
        tx.run("""
            MERGE (d:Device {id:$dev})
            WITH d
            MATCH (a:Audio {uid:$auid})
            MERGE (a)-[:RECORDED_BY]->(d)
        """, dev=row['device'], auid=row['uid'])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--uri', default='bolt://localhost:7687')
    ap.add_argument('--user', default='neo4j')
    ap.add_argument('--pw', required=True)
    ap.add_argument('--jsonl', default='data/audio/dcase_tiny.jsonl')
    ap.add_argument('--source_uid', default='DCASE_TINY')
    ap.add_argument('--activity_uid', default='dcase_import_v1')
    args = ap.parse_args()

    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.pw))
    with driver.session() as sess:
        with sess.begin_transaction() as tx:
            ensure_source_and_activity(tx, args.source_uid, args.activity_uid,
                                       src_kind='audio_set', src_path=args.jsonl, act_kind='import')
        for line in Path(args.jsonl).read_text(encoding='utf-8').splitlines():
            row = json.loads(line)
            with sess.begin_transaction() as tx:
                upsert_audio(tx, row)
                link_generated_by_and_derived_from(tx, row['uid'], args.source_uid, args.activity_uid)
    driver.close()

if __name__ == '__main__':
    main()
