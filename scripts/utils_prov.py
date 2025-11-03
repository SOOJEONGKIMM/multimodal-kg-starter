from neo4j import GraphDatabase

def ensure_source_and_activity(tx, source_uid: str, activity_uid: str, src_kind=None, src_path=None, act_kind=None):
    tx.run("""
        MERGE (s:Source {uid:$suid})
        ON CREATE SET s.kind=$skind, s.path=$spath
        MERGE (a:Activity {uid:$auid})
        ON CREATE SET a.kind=$akind
    """, suid=source_uid, skind=src_kind, spath=src_path, auid=activity_uid, akind=act_kind)

def link_generated_by_and_derived_from(tx, thing_uid: str, source_uid: str, activity_uid: str):
    tx.run("""
        MATCH (x {uid:$xid})
        MATCH (s:Source {uid:$suid})
        MATCH (a:Activity {uid:$auid})
        MERGE (x)-[:DERIVED_FROM]->(s)
        MERGE (x)-[:GENERATED_BY]->(a)
    """, xid=thing_uid, suid=source_uid, auid=activity_uid)
