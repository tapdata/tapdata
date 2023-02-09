## **Connection configuration help**
### **1. Supported version**
Databend v0.9 or later

### **2. Connection configuration
You must have there params to connect databend.

| param            | explain                                                       |
|------------------|---------------------------------------------------------------|
| host             | databend host                                                 |
| user             | the user                                                      |
| password         | the password                                                  |
| port             | the port for databend host                                    |
| database         | which database to sync                                        |
| additionalString | some other params in DSN such as ?xxx=yyy&wait_time_secs=10s& |

If you use databend cloud, you can find these params from [this doc](https://docs.databend.com/using-databend-cloud/warehouses/connecting-a-warehouse).