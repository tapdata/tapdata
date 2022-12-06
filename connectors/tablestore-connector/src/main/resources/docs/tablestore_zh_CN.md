## **连接配置帮助**
### **1. Tablestore 安装说明**
请遵循以下说明以确保在 Tapdata 中成功添加和使用 Elastic Search 数据库。
### **2. 限制说明**
Tapdata系统当前版本 Tablestore 仅支持作为目标。

### **3. 支持版本**
Tablestore 5.13.9
### **4. 配置密钥**
要接入阿里云的表格存储服务，您需要拥有一个有效的访问密钥进行签名认证。目前支持下面三种方式：

#### 阿里云账号的AccessKey ID和AccessKey Secret。创建步骤如下： 
* 在阿里云官网注册阿里云账号。
* 创建AccessKey ID和AccessKey Secret。
#### 被授予访问表格存储权限RAM用户的AccessKey ID和AccessKey Secret。创建步骤如下：
* 使用阿里云账号前往访问控制RAM，创建一个新的RAM用户或者使用已经存在的RAM用户。
* 使用阿里云账号授予RAM用户访问表格存储的权限。
#### RAM用户被授权后，即可使用自己的AccessKey ID和AccessKey Secret访问。从STS获取的临时访问凭证。获取步骤如下：
* 应用的服务器通过访问RAM/STS服务，获取一个临时的AccessKey ID、AccessKey Secret和SecurityToken发送给使用方。
* 使用方使用上述临时密钥访问表格存储服务。

### **5. 注意事项**
* 创建数据表后需要几秒钟进行加载，在此期间对该数据表的读/写数据操作均会失败。请等待数据表加载完毕后再进行数据操作。
* 创建数据表时必须指定数据表的主键。主键包含1个~4个主键列，每一个主键列都有名称和类型。
* 不支持清空表数据。