## 买家下单

#### URL：
POST http://[address]/searcher/find_book

#### Request

##### Body:
```json
{
  "kind": "kind",
  "store_id": "store_id",
  "dict_name": "dict_name",
  "value": "value",
  "page_number": 1,
}
```

##### 属性说明：

变量名 | 类型 | 描述 | 是否可为空
---|---|---|---
kind | string | 查询种类，分为 "one_dict","tags","content" | N
store_id | string | 商铺ID | N
dict_name | string | 所查询项目名称 | N
value | int/string/List[string] | 查询所需对应值，与dict_name对应 | N
page_number | int | 查询结果所需的页数位置 | N

#### Response

Status Code:

码 | 描述
--- | ---
200 | 查询成功
522 | 查询种类异常
523 | 查询参数缺失


##### Body:
```json
{
  "current_page": 1
  "total_page": 20
  "book_information": ["store_id","title"]
}
```

##### 属性说明：

变量名 | 类型 | 描述 | 是否可为空
---|---|---|---
current_page | int | 现在返回给用户的结果所在页数 | N
total_page | int | 查询结果的总页数 | N
book_information | int | 书籍所在店铺以及书名 | N


