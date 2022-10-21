# ongdb-fabric
ONgDB Fabric| ONgDB-v1.0 Fabric组件

>Fabric查询功能v1.0，Neo4j Fabric功能在ONgDB-v1.0中的同类实现。

[Neo4j Fabric官方文档](https://neo4j.com/docs/operations-manual/current/fabric/queries)

## 查询单个图
```sql
//Neo4j
USE example.graphA
MATCH (movie:Movie)
RETURN movie.title AS title
```
```sql
//ONgDB
CALL c1(
    'MATCH (movie:Movie) RETURN movie.title AS title'
) YIELD value
RETURN value.title AS title
```

## 查询多个图
```sql
//Neo4j
USE example.graphA
MATCH (movie:Movie)
RETURN movie.title AS title
  UNION
USE example.graphB
MATCH (movie:Movie)
RETURN movie.title AS title
```
```sql
//ONgDB
CALL c1(
    'MATCH (movie:Movie) RETURN movie.title AS title'
) YIELD value
RETURN value.title AS title
  UNION
CALL c2(
    'MATCH (movie:Movie) RETURN movie.title AS title'
) YIELD value
RETURN value.title AS title
```

## 查询所有图表
```sql
//Neo4j
UNWIND example.graphIds() AS graphId
CALL {
  USE example.graph(graphId)
  MATCH (movie:Movie)
  RETURN movie.title AS title
}
RETURN title
```
```sql
//ONgDB
UNWIND c.ids() AS cId
CALL c(
    cId,
    'MATCH (movie:Movie) RETURN movie.title AS title'
) YIELD value
RETURN value.title AS title
```

## 查询结果聚合
```sql
//Neo4j
UNWIND example.graphIds() AS graphId
CALL {
  USE example.graph(graphId)
  MATCH (movie:Movie)
  RETURN movie.released AS released
}
RETURN min(released) AS earliest
```
```sql
//ONgDB
UNWIND c.ids() AS cId
CALL c(
    cId,
    'MATCH (movie:Movie) RETURN movie.released AS released'
) YIELD value
RETURN min(value.released) AS earliest
```

## 相关子查询
```sql
//Neo4j
CALL {
  USE example.graphA
  MATCH (movie:Movie)
  RETURN max(movie.released) AS usLatest
}
CALL {
  USE example.graphB
  WITH usLatest
  MATCH (movie:Movie)
  WHERE movie.released = usLatest
  RETURN movie
}
RETURN movie
```
```sql
//ONgDB
CALL c1(
    'MATCH (movie:Movie) RETURN max(movie.released) AS usLatest'
) YIELD value
WITH value.usLatest AS usLatest
CALL c2(
    'MATCH (movie:Movie) WHERE movie.released=$usLatest RETURN movie',
    {usLatest:usLatest}
) YIELD value
RETURN value.movie AS movie
```

