# Tapdata Python Sdk

## Install

1. Install python 3.7, pip, ipython By Yourself.
2. Run ```pip install tapdata_cli``` to install sdk.
3. If you use poetry, please run ```poetry add tapdata_cli``` to install sdk.

## Initial

```python
server = "127.0.0.1:3000"
access_code = "3324cfdf-7d3e-4792-bd32-571638d4562f"
from tapdata_cli import cli
cli.init(server, access_code)
```

It will send a request to the server to obtain the identity information and save it as a global variable. Therefore, using multiple 'servers' and 'access' in a multi-threads environment are not secure. Instead, you can use multi-processes.

## Create DataSource

```python
from tapdata_cli import cli
mongo = cli.DataSource("mongodb", name="source")
mongo.uri("mongodb://localhost:8080")
mongo.validate() # available -> True, disabled -> False
mongo.save() # success -> True, Failure -> False
```

TODO: Write docs. 