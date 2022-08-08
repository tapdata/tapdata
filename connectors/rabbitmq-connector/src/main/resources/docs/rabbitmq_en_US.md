## **connection configuration description**

### **1. Rabbitmq configuration description**

- If the queue name is empty, all queues will be loaded by default; If it needs to be specified, it can be separated by commas.
- API port refers to the API call port of rabbitmq default HTTP, which is 8090 by default. (it needs to be specified when loading the queue in full)
- The virtual host needs to match the account to use. It conforms to the permission directory of the account. The default is "/".

### **2. use restrictions**
>- only the message format of JSON object string (such as ` {"Id": 1, "name": "Zhang San"} `) is supported, and jsonbytes, XML and other formats will be supplemented later.
>- you don't need to create a queue in advance
>- the PDK framework restricts the current direct mode of default exchange, and the routing function is not supported.
 
### **3. identify the data type**
- OBJECT
- ARRAY
- NUMBER
- INTEGER
- BOOLEAN
- String (length less than 200)
- TEXT