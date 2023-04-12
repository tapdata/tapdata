## **连接配置帮助**
###  **1. MongoDB Atlas安装说明**
请遵循以下说明以确保在 Tapdata 中成功添加和使用MongoDB Atlas数据库。
> **注意**：连接MongoDB Atlas时，需要按照MongoDB Atlas数据库连接的URI示范格式填写连接串，连接串需要指定：用户名、密码、数据库名。
#### **2. 支持版本**
MongoDB Atlas 5.0.15
> **注意**：请尽量保证资源端数据库和目标数据库都是5.0以上版本。
###  **3. 先决条件**
#### **3.1 作为源数据库**
##### **3.1.1 帐户权限**
如果源端 MongoDB Atlas启用了安全身份验证，则 Tapdata 用于连接源端 MongoDB Atlas的用户帐户必须具有以下内置角色：
```
readAnyDatabase@admin
```
要创建具有上述权限的用户，您可以参考以下：
```
在Atlas管理界面菜单栏中，选择Database Access,之后点击 "ADD NEW DATABASE USER" 按钮，添加用户并赋予相关权限。
```
##### **3.1.2 获取URL**
创建用户后，您可以：
```
菜单栏选择Database，之后依次点击 "Connect" --> "Connect your application"
便可以获取URL。
```
URL格式：
```
mongodb+srv://<username>:<password>@atlascluster.bo1rp4b.mongodb.net/<databaseName>?retryWrites=true&w=majority
```
#### **3.2 作为目标数据库**
#####  **3.2.1 基本配置**
如果目标端 MongoDB Atlas启用了安全身份验证，则 Tapdata 用于连接源端 MongoDB Atlas的用户帐户必须具有以下内置角色：
```
readWriteAnyDatabase@admin
```
#### **4. MongoDB Atlas TLS/SSL配置**
- **启用TLS/SSL**<br>
  请在左侧配置页的 “使用TLS/SSL连接”中选择“是”项进行配置<br>
- **设置MongoDB PemKeyFile**<br>
  点击“选择文件”，选择证书文件，若证书文件有密码保护，则在“私钥密码”中填入密码<br>
- **设置CAFile**<br>
  请在左侧配置页的 “验证服务器证书”中选择“是”<br>
  然后在下方的“认证授权”中点击“选择文件”<br>
- **TSL/SSL参考文档**<br>
  https://www.mongodb.com/docs/atlas/setup-cluster-security/?_ga=2.260151054.2057403045.1679910300-992025068.1669632542#unified-aws-access
