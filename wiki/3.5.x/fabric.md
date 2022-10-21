# Fabric功能

- Features Pending

1. 驱动注册：用户名+bolt地址唯一
2. 全局虚拟ID【待定，ID重复问题】
3. 动态注册c*过程
4. 用户验证

## 过程
### 注册服务ID
```sql
//默认支持c1~c100注册，如果服务ID超过100则需要扩展ongdb-fabric项目
//CALL cRegister('$id','$bolt','$description') 
//    YIELD id,bolt,description
//    RETURN id,bolt,description
```
```sql
//CALL cIds() 
//    YIELD id,bolt,description
//    RETURN id,bolt,description
```

### 服务ID做为参数查询
```sql
CALL c(
    '$cId'
    '$query',
    {$params}
    {$config}
) YIELD value RETURN value
```
```sql
CALL c(
    'c1',
    'MATCH (n) RETURN n LIMIT 10;'
) YIELD value RETURN value
```

### 区分服务ID查询
```sql
CALL c1~100(
    '$query',
    {$queryParas}
) YIELD value RETURN value
```
```sql
CALL c1(
    'MATCH (n) RETURN n LIMIT 10;'
) YIELD value RETURN value
```

## 函数
### 查询已经注册的服务ID
```sql
//返回服务ID
RETURN c.ids() AS graphIds
```


