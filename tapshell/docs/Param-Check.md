# DataSource parameter verification/prompt

[TOC]

The pdk supports the free definition of forms. This module is designed to support updated data sources and facilitate users/developers to use or develop through tapshell.

## DataSource creation interface

Create datasource in uri mode:
```python
from tapdata_cli import cli

d = cli.DataSource("mongodb", name="jerry_test")
d.uri("mongodb://root:xxxx@xxx.xxx.xxx.xxx:xxxxx/source?authSource=admin")
d.save()
```

Create datasource in form mode:
```python
from tapdata_cli import cli

d = cli.DataSource("mongodb", name="jerry_test")
d.database("source").host("xxx.xxx.xxx.xxx").port(27017).user("root").password("xxxx")
d.additionalString("authSource=admin")
d.save()
```

As shown above, in the `uri/form` mode, data source information can be transferred through different interfaces. For example:


- uri mode:
  - uri
- form mode:
  - database
  - host
  - port
  - user
  - password
  - additionalString

How to find these interfaces? Read code? Actually, it's not that troublesome.

## Interface parameter specification

View the `tapshell/params/datasource.py` file. We can see that there are four datasources defined here:

As shown above, we can see that:
- The current built-in data source of the tapshell is mongodb/mysql/postgresql/oracle.
- Each data source can have two modes: uri and form (the two currently supported modes). For example, Mongo supports uri and form, while the rest only supports form.
- Each mode points to a dict structure, which is used to define the parameter specifications of each interface. For example, Mongo's uri mode defines the parameter specifications of three interfaces: isUri/ssl/uri
- Parameter specification description:
  - type: type of parameter
  - require: Whether it is required
  - default: the default value. This value is passed by default only if the requirement is true and no value is passed through the interface
  - option: Optional value
  - reg: regular expression used to specify parameter values
  - desc: Description of the value
  - value: if the value is nested with a `list` or `dict`, the parameter form of the sub element is defined
- If the value is passed through the uri interface, isUri automatically switches to True

Note that you can use interfaces that are not defined in this file. If you define a parameter ak in pdk module, you can use `d.ak (xxxx)` to pass the parameters. You do not need to define interface parameter specifications or anything in this file.
However, for the convenience of others, an interface parameter definition in the above form of the file can let others know how to use the datasource.

## Other parameters

The pdk we saw config above is a datasource specific parameter. There are also some general parameters. You can find `DATASOURCE_CONFIG` data structure in this file.

How to set these general parameters?

```python
from tapdata_cli import cli

d = cli.DataSource("mongodb", name="jerry_test")
d.set({"name": "mongo_custom"})  # set by set()
```

The built-in interfaces such as `set`, `save`, and `delete` will conflict with the above user-defined interfaces.
