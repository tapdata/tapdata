## **Quick Api连接配置帮助**

### 1、填写连接名称（必填）

第一步但不一定是第一步也可以是最后一步，要填写连接名称，因为这是第一个必填项。

### 2、输入从PostMan导出的JSON格式的API文本（必填）

导出的JSON文件中会包含info、item、event、variable四个主要的部分；

####  2.1 info表示这个postman API 文档的基本信息。

####  2.2 item表示这个postman API 文档内包含的API接口信息。需要保证您为待使用API接口具有一定的编辑操作：

##### 2.2.1 表接口声明（必要操作）

您需要在相应的表数据API上个此API名称上添加一些规范化的标签，例如我使获取ZoHo Desk 上门户的工单Tickets,那么我需要在PostMan上对这个获取工单的API进行一定的编辑加工：加工后的API名称应该为

```
  TAP_TABLE[Tickets](PAGE_LIMIT:data)获取工单列表
```

其中包含了以下关键字：

- A、 TAP_TABLE : 建表关键字，表示当前API获取到的数据会形成一张数据表。

- B、 [Tickets] : 指定表名称，一般与TAP_TABLE关键字一起出现，指定建表后的表名称以及API获取到的数据存储到此表。使用[]包裹的一段文字。请合理组织表名称，不建议使用特殊字符，如使用表名称中包含[]这两个字符之一将影响建表后的表名称。

- C、 (PAGE_LIMIT:data) : 指定获取表数据的分页查询类型,以及API调用后返回结果以 data 的值作为表数据，当前API使用的是PAGE_LIMIT分页类型查询数据，表明这个API是根据记录索引和页内偏移进行分页的，具体的分页类型需要您分析API接口后进行指明，不然将会影响查询结果，造成数据误差。以下是提供的分页类型，您可以根据相关API特性进行指定分页类型：

```
    PAGE_SIZE_PAGE_INDEX：适用于使用页码和页内偏移数进行分页。需要搭配 TAP_PAGE_SIZE 和TAP_PAGE_INDEX 标签指定分页参数。
    FROM_TO：适用于使用记录开始索引和结束索引进行分页的。需要单配 TAP_PAGE_FROM 和 TAP_PAGE_TO 标签指定分页参数。
    PAGE_LIMIT：适用于使用记录索引和页内偏移数进行分页的。需要搭配 TAP_PAGE_OFFSET 和 TAP_PAGE_LIMIT 标签指定分页参数。
    PAGE_TOKEN：适用于使用缓存分页Token进行分页的，首页传空，下一页使用上次查询返回的token进行查询 。需要搭配使用 TAP_PAGE_TOKEN 标签指定分页参数，同时使用 TAP_PAGE_SIZE 指定每次分页查询的记录数，使用 TAP_HAS_MORE_PAGE 来描述是否有下一页的字段名称（需要在参数列表中指定这个参数并在参数的描述中添加这个标签）。
    PAGE_NONE：适用于列表返回不分页的普通数据获取。
  ```

- D、 分页参数指定： 以当前查询ZoHo Desk 工单API为例，使用的分页类型为 PAGE_LIMIT，那么分页参数需要在其对应得描述文本中添加相应的参数标签指明 TAP_PAGE_OFFSET 和  TAP_PAGE_LIMIT ，
```
  TAP_PAGE_OFFSET 则对应接口参数 from ,
  TAP_PAGE_LIMIT 对应得接口参数为 limit.
```

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/quickAPI/doc/TAP_TABLE.PNG)

**补充说明：** 以上是ZoHo Desk工单接口声明的案例，Coding的获取事项api名称声明案例为：

    TAP_TABLE[Issues](PAGE_SIZE_PAGE_INDEX:Response.Data.List)获取事项列表

 其语义表示为：设置了事项表名称为Issues，使用了PAGE_SIZE_PAGE_INDEX这个分页逻辑，并指定了API结果中Response.Data.List的数据作为表数据。

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/quickAPI/doc/TAP_TABLE-2.PNG)

##### 2.2.2 登录授权接口声明

您需要使用 TAP_GET_TOKEN 标签声明登录接口。与表数据接口的声明方式一致，需要在接口名称中添加声明标签，登录接口声明标签的关键字是 TAP_GET_TOKEN ，使用此标签表示此数据源在调用API获取数据时会进行access_token的过去判断，那么需要您在连接配置页面进行过期状态描述以及指定access_token获取后的键值匹配。，例如下图表示在Postman对ZoHo Desk进行登录接口的声明：

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/quickAPI/doc/TAP_LOGIN.PNG)

#### 2.3 event表示一些Postman 事件，这个我们基本上使用不到。

#### 2.4 variable表示接口中定义的一些变量，需要保证的是在API上定义的变量一定能在这个variable中找到并存在实际且正确的值，否则这个使用了无法找到或错误值变量的API接口将在不久的将来调用失败。

### 3、填写access_token过期状态描述（选填）

注：这个输入项作为选填的原因是：

  部分Saas平台提供的OpenAPI使用的是永久性的访问令牌，无需考虑token过期的情况，例如Coding。但对于使用临时令牌访问OpenAPI的Saas平台，需要你填写这个输入项，否则可能造成不可预知的后果。
  填写access_token过期状态描述。(这里的access_token泛指API接口访问令牌,每个Saas的名称可能并不一致)

- 3.1 access_token过期状态是指您的API访问过期后，在调用指定接口后Saas平台返回的访问失败状态。

例如我们在调用ZoHo获取工单时，access_token过期了，此时返回结果如下图所示，那么您可以将过期状态描述为  errorCode=INVALID_OAUTH ，这样再执行API时可以自动根据返回结果识别为token过期实现自动刷新token。

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/quickAPI/doc/TAP_TABLE-ZoHo.PNG)

- 3.2 这个状态描述需要您手动通过PostMan访问API总结出来（因为我们无法预知这些Saas平台在access_token过期后以何种响应结果返回）；

- 3.3 在PostMan对登录（获取API访问权限）的API接口进行声明，当执行API过程中发现了access_token过期后悔调用这个指定的API进行access_token刷新，这个登录接口需要在接口的名称上加上 TAP_GET_TOKEN 这样一个标志性文字。例如：对ZoHo令牌刷新接口的名称为 “TAP_GET_TOKEN刷新AccessToken-登录” ,其加上了 TAP_GET_TOKEN（见左上角） 表示此接口用于实现自动令牌刷新操作。

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/quickAPI/doc/TAP_LOGIN-ZoHo.PNG)

- 3.4 过期状态描述有以下描述规则：

```properties
//支持直接指定值
body.errorCode=NO AUTH

//支持且关系判断，使用&&连接
body.errorCode=NO AUTH&&body.code=500021

//支持或关系判断，并列的或关系换行表示
body.code=500021
body.code=500021

//支持范围值
body.code=[50000,51000]

//可考虑支持正则表达式
body.errorCode=regex('$/[0-1|a-z]{5}/$')

header.httpCode=401

code=401
```

### 4、指定自动刷新token后的键值匹配规则（选填）

注：

这个输入项作为选填的原因在配置token状态描述后需要你填写这个输入项，否则可能造成不可预知的后果，

虽然系统会为您在登录接口返回值中模糊匹配关键数据到全局参数列表中，但是无法保证模糊匹配上的token能正确赋值到你自定义的token参数上。

因为模糊匹配规则只是个经验值，不能保证100%成功匹配上，大致的匹配思路如下：

（1）在登录授权接口的返回值中找出可能是token的字段及其对应得值。

```
根据关键字access_toke，在接口返回值中找到符合条件的token；

如果第一步没有在则使用token关键字进行搜索，如果存在多个这样的值，那么在全局参数中找出与访问令牌值得格式最相近的作为访问令牌；

如果依然没有找到，则使用token关键字进行上一步操作。

最终如果没办法找出则抛错并产生提示，需要手动指定返回结果与全局变量中的token键值规则。

```
（2）在全局参数中找出可能是访问令牌的属性并重新赋值。
找出全局参数列表中在接口Headers中Authorization参数使用的变量。


需要指明刷新token的API在获取到的结果中哪一个键值对应文档中描述的AccessToken。

例如：
```
我在ZoHo Desk 中使用Postman导出的接口集合中，
我使用了一个全局参数accessToken来声明了一个全局变量，
这个变量应用在所有的API上，用于API的访问令牌。

zoho desk的登录接口返回的访问令牌名称叫做access_tokon，
此时我们需要在此声明 accessToken=access_token 。
```

### 数据源支持

- 1. 在PostMan中进行API的声明，至少包含了一个以上的TAP_TABLE声明后的API，否则使用这个创建的连接将无法扫描到任何表，TAP_TABLE需要同时声明表名称，分页类型，分页参数指定。否则会出现错误的结果。

- 2. 可能需要配置登录授权API，如果配置了登录授权API，则需要您在连接配置页面配置Token过期规则 以及指定获取Token的接口中返回结果与全局变量中token变量对应关系。

- 3. 支持大部分场景下的Saas数据源，如：
    
   使用永久Token进行OpenAPI调用的：Coding等。

   使用动态刷新访问令牌的OpenAPI调用的：ZoHo Desk等。

