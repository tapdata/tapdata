## **連接配置幫助**

### **Javascript Built-in Function**

### **1. Http**

```js
Http Return Instructions

// data為返回的body，可能是array或object或string

{code:200, data:[]}
```

```javascript
rest.get(url, header)
rest.get(url, header, returnType)
rest.get(url, header, connectTimeOut, readTimeOut)
rest.get(url, header, returnType, connectTimeOut, readTimeOut)

// 調用http的 get 方法
// returnType: 返回的結果類型，默認為: array，可选："object", "string", "array"
// connectTimeOut：連接超時時間，單位毫秒(ms)，默認為 10000 ms，需要指定連接超時時間時可以使用該參數
// readTimeOut：讀取超時時間，單位毫秒(ms)，默認為 30000 ms，需要指定讀取超時時間時可以使用該參數

var result = rest.get('http://127.0.0.1:1234/users?id=1', {}, 'array', 30, 300);
```

```javascript
rest.post(url, parameters)
rest.post(url, parameters, headers, returnType)
rest.post(url, parameters, connectTimeOut, readTimeOut)
rest.post(url, parameters, headers, returnType, connectTimeOut, readTimeOut)

// 調用http的 post 方法
// returnType: 返回的結果類型，默認為: array，可选："object", "string", "array"
// connectTimeOut：連接超時時間，單位毫秒(ms)，默認為 10000 ms，需要指定連接超時時間時可以使用該參數
// readTimeOut：讀取超時時間，單位毫秒(ms)，默認為 30000 ms，需要指定讀取超時時間時可以使用該參數

var result = rest.post('http://127.0.0.1:1234/users/find', {}, {}, 'array', 30, 300);
```

```javascript
rest.patch(url, parameters)
rest.patch(url, parameters, headers)
rest.patch(url, parameters, connectTimeOut, readTimeOut)
rest.patch(url, parameters, headers, connectTimeOut, readTimeOut)

// 調用http的 patch 方法
// connectTimeOut：連接超時時間，單位毫秒(ms)，默認為 10000 ms，需要指定連接超時時間時可以使用該參數
// readTimeOut：讀取超時時間，單位毫秒(ms)，默認為 30000 ms，需要指定讀取超時時間時可以使用該參數

var result = rest.patch('http://127.0.0.1:1234/users?where[user_id]=1', {status: 0}, {}, 30, 300);
```

```javascript
rest.delete(url)
rest.delete(url, headers)
rest.delete(url, connectTimeOut, readTimeOut)
rest.delete(url, headers, connectTimeOut, readTimeOut)

// 調用http的 delete 方法
// connectTimeOut：連接超時時間，單位毫秒(ms)，默認為 10000 ms，需要指定連接超時時間時可以使用該參數
// readTimeOut：讀取超時時間，單位毫秒(ms)，默認為 30000 ms，需要指定讀取超時時間時可以使用該參數

var result = rest.delete('http://127.0.0.1:1234/users?where[user_id]=1', {}, 30, 300);
```

### **2. MongoDB**

```js
mongo.getData(uri, collection)
mongo.getData(uri, collection, filter)
mongo.getData(uri, collection, filter, limit, sort)

// MongoDB 查詢數據

var result = mongo.getData('mongodb://127.0.0.1:27017/test', 'users', {id: 1}, 10, {add_time: -1});
```

```js
mongo.insert(url, collection, inserts)

// MongoDB 插入數據
// inserts 表示插入的數據，可以傳入數組或者對象

mongo.insert('mongodb://127.0.0.1:27017/test', 'users', [{id: 1, name: 'test1'}, {id: 2, name: 'test2'}]);
```

```js
mongo.update(url, collection, filter, update)

// MongoDB更新數據

var modifyCount = mongo.update('mongodb://127.0.0.1:27017/test', 'users', {id: 1}, {name: 'test3'});
```

```js
mongo.delete(url, collection, filter)

// MongoDB刪除數據

var deleteCount = mongo.delete('mongodb://127.0.0.1:27017/test', 'users', {id: 1});
```
