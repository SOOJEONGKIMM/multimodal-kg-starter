// 이미지와 개념/캡션
MATCH (i:Image)
OPTIONAL MATCH (i)-[:DESCRIBED_BY]->(t:TextChunk)
OPTIONAL MATCH (c:Concept)-[:APPEARS_IN]->(i)
RETURN i.uid AS image, i.file AS file, collect(DISTINCT c.name) AS concepts, t.text AS caption
LIMIT 10;

// SOSA 관측 개수 요약
MATCH (o:Observation)-[:MADE_BY_SENSOR]->(s:Sensor)
OPTIONAL MATCH (o)-[:OF_PROPERTY]->(p:Property)
RETURN s.uid AS sensor, p.name AS property, count(o) AS obs_count
ORDER BY obs_count DESC LIMIT 10;

// 도시/장면별 오디오 분포
MATCH (a:Audio)-[:RECORDED_IN]->(city:City)
OPTIONAL MATCH (a)-[:HAS_SCENE]->(sc:Scene)
RETURN city.name AS city, collect(DISTINCT sc.name) AS scenes, count(*) AS n
ORDER BY n DESC LIMIT 10;
