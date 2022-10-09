# 数据源参数校验/提示

[TOC]

pdk支持自由定义表单，该模块旨在支持更新的数据源，方便用户/开发者使用tapshell或通过tapshell进行开发。


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

查看`tapshell/params/datasource.py`文件，我们可以看到这里定义了4个数据源：
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

内置的set、save、delete等接口会和上述自定义接口冲突，这是需要注意的地方。
