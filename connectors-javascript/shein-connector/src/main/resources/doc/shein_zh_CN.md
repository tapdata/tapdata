## SHEIN 作为源

官方网站：https://openapi-portal.sheincorp.cn/#/home/1/999999

### 使用须知

使用SHEIN作为源前，您需要前往SHEIN联系您对应的系统对接人：
- 申请开通接入账号
- 向SHEIN具体系统对接人获取openKeyId、secretKey

#### 创建连接

创建连接需要配置的连接属性包含以下部分：

- Open Key ID：申请开通接入账号后（由 SHEIN 后台录入，提供给用户 openKeyId），将获取到的openKeyId填入此输入框
- Secret Key：申请开通接入账号（由 SHEIN 后台录入，提供给用户 secretKey），将获取到的secretKey填入到此输入款

#### 作为源

- 您可以获取到 商家服务-采购 模块中的采购订单，包含急采、备货。
- SHEIN数据源作为目标，支持全量获取采购单和增量轮询获取采购单。