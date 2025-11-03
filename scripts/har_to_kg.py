import json, argparse
from pathlib import Path
from neo4j import GraphDatabase
from utils_prov import ensure_source_and_activity, link_generated_by_and_derived_from

def upsert_observation(tx, row):
    tx.run("""
        MERGE (o:Observation {uid:$uid})
        ON CREATE SET o.time=$time, o.value=$val, o.unit=$unit
        ON MATCH SET o.time=COALESCE(o.time,$time), o.value=COALESCE(o.value,$val), o.unit=COALESCE(o.unit,$unit)
    """, uid=row['uid'], time=row.get('time'), val=row.get('value'), unit=row.get('unit'))

    tx.run("""
        MERGE (s:Sensor {uid:$sid})
        WITH s
        MATCH (o:Observation {uid:$oid})
        MERGE (o)-[:MADE_BY_SENSOR]->(s)
    """, sid=row.get('sensor_uid'), oid=row['uid'])

    if row.get('property'):
        tx.run("""
            MERGE (p:Property {name:$prop})
            WITH p
            MATCH (o:Observation {uid:$oid})
            MERGE (o)-[:OF_PROPERTY]->(p)
        """, prop=row['property'], oid=row['uid'])

    if row.get('subject'):
        tx.run("""
            MERGE (f:FeatureOfInterest {name:$subj})
            WITH f
            MATCH (o:Observation {uid:$oid})
            MERGE (o)-[:HAS_FEATURE_OF_INTEREST]->(f)
        """, subj=row['subject'], oid=row['uid'])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--uri', default='bolt://localhost:7687')
    ap.add_argument('--user', default='neo4j')
    ap.add_argument('--pw', required=True)
    ap.add_argument('--jsonl', default='data/signals/har_tiny.jsonl')
    ap.add_argument('--source_uid', default='HAR_TINY')
    ap.add_argument('--activity_uid', default='har_import_v1')
    args = ap.parse_args()

    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.pw))
    with driver.session() as sess:
        with sess.begin_transaction() as tx:
            ensure_source_and_activity(tx, args.source_uid, args.activity_uid,
                                       src_kind='signal_set', src_path=args.jsonl, act_kind='import')
        for line in Path(args.jsonl).read_text(encoding='utf-8').splitlines():
            row = json.loads(line)
            with sess.begin_transaction() as tx:
                upsert_observation(tx, row)
                link_generated_by_and_derived_from(tx, row['uid'], args.source_uid, args.activity_uid)
    driver.close()

if __name__ == '__main__':
    main()
