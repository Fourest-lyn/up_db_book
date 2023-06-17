# Report

## 数据库结构
本项目在原有book_store的项目基础上，将原来使用的mogodb数据库，转变成为使用PostgreSQL数据库来储存数据。由于变成了新的关系数据库系统，因此也对数据库结构进行了大规模的调整。下面是数据库包含的内容：

### users表格
| user_id | password | balance | token | terminal |
| -------- | -------- | -------- | -------- | -------- |
| string | string | integer | string | string |

这个表格主要用于记录不论是书店拥有者还是购书者的个人账户信息。每一个用户由唯一的`user_id`确定。

### stores表格：

| store_id | user_id | 
| -------- | -------- | 
| string | string | 

这个表格主要用来记录一个商店与与其对应的拥有者的关系，每个商店有唯一的`store_id`确定。

### book_list表格：

| store_id | book_id | book_info_id | stock_level |
| -------- | -------- | -------- | -------- | 
| string | string | integer | integer |

这个表格记录了一本书在书店内有关自身库存以及内容的信息，内容以id的形式记录在此表中，可以到后面的表格中查询具体内容。

### book_info表格:

| book_info_id | store_id | book_id | ...(other data) | 
| -------- | -------- | -------- | -------- | 
| integer | string | string | ... | 

此表记录了一本书具体的内容信息，拥有一个唯一的主键`book_info_id`。

### book_tags表格：

| book_info_id | tag | 
| -------- | -------- | 
| integer | string | 

这个表格是上面的`book_info`表格的拓展，用于记录每一本书所具有的多个的`tag`。

### new_order表格：

| order_id | user_id | store_id | create_time | status |
| -------- | -------- | -------- | -------- | -------- |
| string | string | string | integer | integer |

这个表格用于记录购书订单。每个订单由主键`order_id`唯一确定。

### order_book表格：

| order_id | book_id | count | price |
| -------- | -------- | -------- | -------- |
| string | string | integer | integer |

这个表格是上面的表格`new_order`的拓展，用于记录每个订单内每一本书的具体信息，如数量与价格。

数据库的检索优化主要由各个表格的unique主键完成，作为最常用到的检索条件作为索引简化搜索。

以上便是该数据库的结构。ER图将会附在本项目下的picture文件夹内。

-----

## 事务处理与接口
本项目的所有过程都在程序内分模块处理，由不同的模块与数据库负责执行与记录。
例如`login`操作，只需要查询`users`数据库以及利用`user.py`等文件即可执行。
更加具体的事务处理的分配以及接口构筑可以参考`doc`文件夹下属文件。

-----

## 测试
本项目的测试延续了原来项目的测试方案，每一个测试点均能单独通过。具体的内容可以参考`doc`文件夹下属文件。最终测试结果,项目的主要后端`be`文件夹的覆盖率在`Stmts,Miss,Cover=1129,249,78%`左右。
测试通过`pytest fe/test/test_xxx.py`代码实现，所有测试点都能单独通过。

-----

## git使用
本项目的git地址为https://github.com/Fourest-lyn/up_db_book.git
现已开放为public，可以查询版本进度。由于本项目是由原有项目基础构筑的，所以最开始的构筑过程会有缺失，后续修改为sql的部分全过程在commit信息中。



