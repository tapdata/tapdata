## **connection configuration description**

### **1. ActiveMQ configuration description**

- If the queue name is empty, all queues will be loaded by default; If it needs to be specified, it can be separated by commas.
- MQ connection string (brokerurl) format: tcp://[host]:[port].

### **2. use restrictions**

>- only the message format of JSON object string (such as ` {"Id": 1, "name": "Zhang San"} `) is supported, and jsonbytes, XML and other formats will be supplemented later.
>- you don't need to create a queue in advance
>- the PDK framework is limited and the topic method can not support the full volume, so topic is temporarily unavailable.

### **3. identify the data type**
- OBJECT
- ARRAY
- NUMBER
- INTEGER
- BOOLEAN
- String (length less than 200)
- TEXT