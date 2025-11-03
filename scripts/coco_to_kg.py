import json, argparse
from pathlib import Path
from neo4j import GraphDatabase
from utils_prov import ensure_source_and_activity, link_generated_by_and_derived_from

def upsert_image(tx, row):
    tx.run("""
        MERGE (i:Image {uid:$uid})
        ON CREATE SET i.file=$file, i.timestamp=$ts
        ON MATCH SET i.file=COALESCE(i.file,$file), i.timestamp=COALESCE(i.timestamp,$ts)
    """, uid=row['uid'], file=row.get('file'), ts=row.get('timestamp'))

    if row.get('caption'):
        cap_uid = f"cap::{row['uid']}"
        tx.run("""
            MERGE (t:TextChunk {uid:$uid})
            ON CREATE SET t.text=$text
            ON MATCH SET t.text=COALESCE(t.text,$text)
            WITH t
            MATCH (i:Image {uid:$img_uid})
            MERGE (i)-[:DESCRIBED_BY]->(t)
        """, uid=cap_uid, text=row['caption'], img_uid=row['uid'])

    for cat in row.get('concepts', []):
        tx.run("""
            MERGE (c:Concept {name:$name})
            ON CREATE SET c.type='coco_category'
            WITH c
            MATCH (i:Image {uid:$img_uid})
            MERGE (c)-[:APPEARS_IN]->(i)
        """, name=cat, img_uid=row['uid'])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--uri', default='bolt://localhost:7687')
    ap.add_argument('--user', default='neo4j')
    ap.add_argument('--pw', required=True)
    ap.add_argument('--jsonl', default='data/images/coco_tiny.jsonl')
    ap.add_argument('--source_uid', default='COCO_TINY')
    ap.add_argument('--activity_uid', default='coco_import_v1')
    args = ap.parse_args()

    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.pw))
    with driver.session() as sess:
        with sess.begin_transaction() as tx:
            ensure_source_and_activity(tx, args.source_uid, args.activity_uid,
                                       src_kind='image_set', src_path=args.jsonl, act_kind='import')
        for line in Path(args.jsonl).read_text(encoding='utf-8').splitlines():
            row = json.loads(line)
            with sess.begin_transaction() as tx:
                upsert_image(tx, row)
                link_generated_by_and_derived_from(tx, row['uid'], args.source_uid, args.activity_uid)
    driver.close()

if __name__ == '__main__':
    main()
