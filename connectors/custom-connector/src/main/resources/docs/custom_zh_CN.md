## **连接配置帮助**

### **Javascript Built-in Function**

### **1. Http**

```js
Http Return Instructions

// data为返回的body，可能是array或object或string

{code:200, data:[]}
```

```javascript
rest.get(url, header)
rest.get(url, header, returnType)
rest.get(url, header, connectTimeOut, readTimeOut)
rest.get(url, header, returnType, connectTimeOut, readTimeOut)

// 调用http的 get 方法
// returnType: 返回的结果类型，默认为: array，可选："object", "string", "array"
// connectTimeOut：连接超时时间，单位毫秒(ms)，默认为 10000 ms，需要指定连接超时时间时可以使用该参数
// readTimeOut：读取超时时间，单位毫秒(ms)，默认为 30000 ms，需要指定读取超时时间时可以使用该参数

var result = rest.get('http://127.0.0.1:1234/users?id=1', {}, 'array', 30, 300);
```

```javascript
rest.post(url, parameters)
rest.post(url, parameters, headers, returnType)
rest.post(url, parameters, connectTimeOut, readTimeOut)
rest.post(url, parameters, headers, returnType, connectTimeOut, readTimeOut)

// 调用http的 post 方法
// returnType: 返回的结果类型，默认为: array，可选："object", "string", "array"
// connectTimeOut：连接超时时间，单位毫秒(ms)，默认为 10000 ms，需要指定连接超时时间时可以使用该参数
// readTimeOut：读取超时时间，单位毫秒(ms)，默认为 30000 ms，需要指定读取超时时间时可以使用该参数

var result = rest.post('http://127.0.0.1:1234/users/find', {}, {}, 'array', 30, 300);
```

```javascript
rest.patch(url, parameters)
rest.patch(url, parameters, headers)
rest.patch(url, parameters, connectTimeOut, readTimeOut)
rest.patch(url, parameters, headers, connectTimeOut, readTimeOut)

// 调用http的 patch 方法
// connectTimeOut：连接超时时间，单位毫秒(ms)，默认为 10000 ms，需要指定连接超时时间时可以使用该参数
// readTimeOut：读取超时时间，单位毫秒(ms)，默认为 30000 ms，需要指定读取超时时间时可以使用该参数

var result = rest.patch('http://127.0.0.1:1234/users?where[user_id]=1', {status: 0}, {}, 30, 300);
```

```javascript
rest.delete(url)
rest.delete(url, headers)
rest.delete(url, connectTimeOut, readTimeOut)
rest.delete(url, headers, connectTimeOut, readTimeOut)

// 调用http的 delete 方法
// connectTimeOut：连接超时时间，单位毫秒(ms)，默认为 10000 ms，需要指定连接超时时间时可以使用该参数
// readTimeOut：读取超时时间，单位毫秒(ms)，默认为 30000 ms，需要指定读取超时时间时可以使用该参数

var result = rest.delete('http://127.0.0.1:1234/users?where[user_id]=1', {}, 30, 300);
```

### **2. MongoDB**

```js
mongo.getData(uri, collection)
mongo.getData(uri, collection, filter)
mongo.getData(uri, collection, filter, limit, sort)

// MongoDB 查询数据

var result = mongo.getData('mongodb://127.0.0.1:27017/test', 'users', {id: 1}, 10, {add_time: -1});
```

```js
mongo.insert(url, collection, inserts)

// MongoDB 插入数据
// inserts 表示插入的数据，可以传入数组或者对象

mongo.insert('mongodb://127.0.0.1:27017/test', 'users', [{id: 1, name: 'test1'}, {id: 2, name: 'test2'}]);
```

```js
mongo.update(url, collection, filter, update)

// MongoDB更新数据

var modifyCount = mongo.update('mongodb://127.0.0.1:27017/test', 'users', {id: 1}, {name: 'test3'});
```

```js
mongo.delete(url, collection, filter)

// MongoDB删除数据

var deleteCount = mongo.delete('mongodb://127.0.0.1:27017/test', 'users', {id: 1});
```
