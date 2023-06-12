## 买家下单

#### URL：
POST http://[address]/buyer/new_order

#### Request

##### Header:

key | 类型 | 描述 | 是否可为空
---|---|---|---
token | string | 登录产生的会话标识 | N

##### Body:
```json
{
  "user_id": "buyer_id",
  "store_id": "store_id",
  "books": [
    {
      "id": "1000067",
      "count": 1
    },
    {
      "id": "1000134",
      "count": 4
    }
  ]
  "create_time": 1,
}
```

##### 属性说明：

变量名 | 类型 | 描述 | 是否可为空
---|---|---|---
user_id | string | 买家用户ID | N
store_id | string | 商铺ID | N
books | class | 书籍购买列表 | N
create_time | int | 下单时间 | N

books数组：

变量名 | 类型 | 描述 | 是否可为空
---|---|---|---
id | string | 书籍的ID | N
count | string | 购买数量 | N


#### Response

Status Code:

码 | 描述
--- | ---
200 | 下单成功
5XX | 买家用户ID不存在
5XX | 商铺ID不存在
5XX | 购买的图书不存在
5XX | 商品库存不足

##### Body:
```json
{
  "order_id": "uuid"
}
```

##### 属性说明：

变量名 | 类型 | 描述 | 是否可为空
---|---|---|---
order_id | string | 订单号，只有返回200时才有效 | N


## 买家付款

#### URL：
POST http://[address]/buyer/payment

#### Request

##### Body:
```json
{
  "user_id": "buyer_id",
  "order_id": "order_id",
  "password": "password"
}
```

##### 属性说明：

变量名 | 类型 | 描述 | 是否可为空
---|---|---|---
user_id | string | 买家用户ID | N
order_id | string | 订单ID | N
password | string | 买家用户密码 | N 


#### Response

Status Code:

码 | 描述
--- | ---
200 | 付款成功
5XX | 账户余额不足
5XX | 无效参数
401 | 授权失败 


## 买家充值

#### URL：
POST http://[address]/buyer/add_funds

#### Request



##### Body:
```json
{
  "user_id": "user_id",
  "password": "password",
  "add_value": 10
}
```

##### 属性说明：

key | 类型 | 描述 | 是否可为空
---|---|---|---
user_id | string | 买家用户ID | N
password | string | 用户密码 | N
add_value | int | 充值金额，以分为单位 | N

##### Status Code:

码 | 描述
--- | ---
200 | 充值成功
401 | 授权失败
5XX | 无效参数

## 买家收货

#### URL：

POST http://[address]/buyer/receive_order

#### Request

##### Body:

```json
{
     "user_id": "user_id",
     "order_id": "order_id",
 }
```

##### 属性说明：

key|类型|描述 | 是否可为空
---|---|---|---
user_id|string|买家用户ID | N 
order_id|string|订单号 | N 

#### Response

##### Status Code:

码 | 描述
--- | ---
200 | 收货成功 
401 | 授权失败
5XX | 无效参数
520 | 订单状态不正确，只有已发货的订单支持收货 

## 买家查询订单

#### URL：

POST http://[address]/buyer/receive_order

#### Request

##### Body:

```json
{
     "user_id": "user_id",
     "order_id": "order_id",
 }
```

##### 属性说明：

key|类型|描述 | 是否可为空
---|---|---|---
user_id|string|买家用户ID | N 
order_id|string|订单号 | N 

#### Response

##### Status Code:

码 | 描述
--- | ---
200 | 查询成功 
401 | 授权失败
5XX | 无效参数

##### Body:

```json
{
  "order": {
      "order_id": "order_id",
      "user_id": "user_id",
      "store_id": "store_id",
      "book_list": List[{
          "book_id": "book_id", 
          "count": "count", 
          "price": "price",
      }],
      "status": Union[INIT=0, PAID=1, SHIPPED=2, RECEIVED=3, CANCELLED=4],
  }
}
```

##### 属性说明：

| 变量名    | 类型                    | 描述                                                         | 是否可为空 |
| --------- | ----------------------- | ------------------------------------------------------------ | ---------- |
| order_id  | string                  | 订单号，只有返回200时才有效                                  | N          |
| user_id   | string                  | 该用户id                                                     | N          |
| store_id  | string                  | 下单的店铺id                                                 | N          |
| book_list | List                    | 一个列表，每一项由一个元组book_item构成                      | N          |
| book_item | Tuple[string, int, int] | 购买的书籍id、数量、以及价格                                 | N          |
| status    | int                     | 代表当前订单的状态，分别为已提交、已付款、已发货、已收货、已取消 | N          |

## 买家查询所有订单编号

#### URL：

POST http://[address]/buyer/query_order_id_list

#### Request

##### Body:

```json
{
     "user_id": "user_id",
     "password": "password",
 }
```

##### 属性说明：

key|类型|描述 | 是否可为空
---|---|---|---
user_id|string|买家用户ID | N 
password|string|用户密码 | N 

#### Response

##### Status Code:

码 | 描述
--- | ---
200 | 查询成功 
401 | 授权失败
5XX | 无效参数

##### Body:

```json
{
    "order_id_list": List["order_id"]
}
```

##### 属性说明：

key|类型|描述 | 是否可为空
---|---|---|---
order_id_list|List[string]|所有买家订单id | N 

## 买家取消订单

#### URL：

POST http://[address]/buyer/cancel_order

#### Request

##### Body:

```json
{
     "user_id": "user_id",
     "password": "password",
     "order_id": "order_id",
 }
```

##### 属性说明：

key|类型|描述 | 是否可为空
---|---|---|---
user_id|string|买家用户ID | N 
password|string|用户密码 | N 
order_id|string|订单号 | N 

#### Response

##### Status Code:

码 | 描述
--- | ---
200 | 查询成功 
401 | 授权失败
5XX | 无效参数
520 | 订单状态不正确，只有未付款的订单支持取消 
