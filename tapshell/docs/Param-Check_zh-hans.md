# 数据源接口说明

[TOC]

pdk支持自由定义表单，该模块旨在支持更新的数据源，方便用户/开发者使用tapshell/sdk或通过tapshell/sdk进行开发。


## 数据源创建接口

以uri方式创建数据源：
```python
from tapdata_cli import cli

d = cli.DataSource("mongodb", name="jerry_test")
d.uri("mongodb://root:xxxx@xxx.xxx.xxx.xxx:xxxxx/source?authSource=admin")
d.save()
```

以form表单方式创建数据源：
```python
from tapdata_cli import cli

d = cli.DataSource("mongodb", name="jerry_test")
d.database("source").host("xxx.xxx.xxx.xxx").port(27017).user("root").password("xxxx")
d.additionalString("authSource=admin")
d.save()
```

如上所示，`uri`/`form`模式下，可以通过不同的接口来传递数据源的信息。如：

- uri模式
  - uri
- form模式
  - database
  - host
  - port
  - user
  - password
  - additionalString
  
如何找到这些接口呢？翻代码？实际上并没有那么麻烦。

## 接口参数规范

查看[这个文件](https://github.com/tapdata/tapdata/blob/feat-tapshell/tapshell/tapdata_cli/params/datasource.py) ，我们可以看到这里定义了一系列数据源：
```python
pdk_config = {
    "mongodb": {
        "uri": PDK_MONGO_URI,
        "form": PDK_MONGO_FORM,
    },
    "mysql": {
        "form": PDK_MYSQL_FORM,
    },
    "postgresql": {
        "form": PDK_POSTGRESQL_FORM,
    },
    "oracle": {
        "form": PDK_ORACLE_FORM,
    },
}
```

如上所示，我们可以看到：
- tapshell目前内置的数据源：mongodb/mysql/postgresql/oracle。
- 每个数据源可以有两种模式，uri和form两种模式（目前支持的两种模式），如mongo支持uri和form两种模式，而其余则只支持form一种模式。
- 每个模式指向一个dict结构，用来定义各个接口的参数规范，如mongo的uri模式定义了三个接口的参数规范：isUri/ssl/uri
- 参数规范说明：
  - type：参数的类型
  - require：是否必传
  - default：默认值，仅当require为True且未通过接口传值时，默认传递该值
  - option：可选值
  - reg：正则表达式，用来规范参数值
  - desc：该值的说明
  - value：当该值嵌套一个list或者dict时，定义子元素的参数形式
- 当通过uri接口传递值，则isUri自动切换为True

需要注意的是，你可以使用未被该文件定义的接口，如果您在pdk定义了一个参数ak,则可以使用`d.ak(xxxx)`来传递参数，无需在该文件定义接口参数规范。
但是为了方便别人使用，在该文件以上述形式做一个接口参数定义更能让其他人知道如何使用该数据源。

## 其他参数

上面我们看到的pdk_config是特异于数据源的参数，还有一些通用的参数，可以在该文件找到`DATASOURCE_CONFIG`数据结构。

如何设置这些通用参数？

```python
from tapdata_cli import cli

d = cli.DataSource("mongodb", name="jerry_test")
d.set({"name": "mongo_custom"})  # 通过set接口来设置通用参数
```

例如修改连接器类型，我们可以看到`DATASOURCE_CONFIG`结构体存在如下设置: 
```python
DATASOURCE_CONFIG = {
    ...
    "connection_type": {
        "type": str, "default": "source_and_target", "require": True,
        "option": ["source", "target", "source_and_target"],
        "desc": "This data connection can be used as source and target at the same time",
    },
    ...
}
```

其中我们可以看到`connection_type`字段的option值可以为`source/target/source_and_target`对应三种连接器类型，所以我们可以这样来修改值：
```python
from tapdata_cli import cli

d = cli.DataSource("mongodb", name="jerry_test")
d.set({"connection_type": "target"})  # 将数据源类型修改为target
```

除此之外，数据源类型还可以在数据源初始化的时候直接赋值，因为DataSource类提供了type的默认参数（source_and_target）:
```python
from tapdata_cli import cli

d = cli.DataSource("mongodb", name="jerry_test", type="target")
d.save()
```

内置的set、save、delete等接口会和上述自定义接口冲突，这是需要注意的地方。

## Python-Sdk接口变动说明

从Python-Sdk 2.2.15升级到Python-Sdk 2.2.20，有一些接口变动：

1. 统一使用DataSource类作为数据源操作的统一接口，**废弃原有的Oracle/Kafka/MongoDB/Mysql/Postgres接口**。
2. Oracle数据源接口变动：
- port: 参数类型由str/int变为int
- username: 变为user
- database: 如果thinType设置为SERVICE_NAME（默认为SID），则database变为SERVICE_NAME
3. Kafka数据源接口变动：
- uri: 变为nameSrvAddr
4. MongoDB数据源form模式下/Mysql数据源接口变动：
- db: 变为database
- port: 参数类型由str/int变为int
- additionalString: 变为props
5. postgresql数据源接口变动:
- set_log_decorder_plugin: 变为logPluginName
- port: 参数类型由str/int变为int
