### 写在前面
如果您感兴趣的话，不妨前往Google提供的文档，详细了解全部内容：

- 说明文档：[https://cloud.google.com/docs](https://cloud.google.com/docs)
- 操作文档：[https://cloud.google.com/bigquery/docs/](https://cloud.google.com/bigquery/docs/)
- 创建和管理服务账号：[https://cloud.google.com/iam/docs/creating-managing-service-accounts](https://cloud.google.com/iam/docs/creating-managing-service-accounts)

当然您也可以浏览以下内容，快速上手BigQuery数据源的配置流程。

---

### 1.属性说明

1. 服务账号：您需要手动前往BigQuery控制台设置规则并创建服务账号，作为数据访问凭据；

2. 数据集ID：需要您确认BigQuery数据源对应的数据集并输入；

---

### 2.配置步骤
#### 2.1 基础配置

获取 **服务账号**：

 - 1.请前往BigQuery控制台，进入凭据管理操作界面：[https://console.cloud.google.com/apis/credentials](https://console.cloud.google.com/apis/credentials)
    
 - 2.如果您已经配置过相应的服务账号（***请直接跳过2-6这些步骤直接从第7步开始***），您此刻需要新建一个服务账号。点击菜单栏中的 **CREATE CREDENTIAL**选项：
 
 ![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/doc/BigQuery/serviceAccount1.png)
 
 - 3.选择 **Service Account**，进行服务账号的创建：
 
 ![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/doc/BigQuery/serviceAccount2.png)
 
 - 4.分别填写服务账号的基本信息：
 
 ![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/doc/BigQuery/serviceAccount3.png)
 
 - 5.将此服务帐户关联到项目，并配置其访问权限规则，我们这里需要选择BigQuery下的BigQuery Admin权限；
 
 ![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/doc/BigQuery/serviceAccount4.png)
 ![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/doc/BigQuery/serviceAccount5.png)
 
 - 6.配置完成后，点击创建。我们会回到Credentital页面，可以在Service Account表格中看到我们刚刚创建好的服务账号：
 
 ![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/doc/BigQuery/serviceAccount6.png)
 
 - 7.点击这个创建好的Service account，进入Service account.此时我们开始配置访问秘钥，也就是我们创建数据源是需要用到的关键信息。我们选择Key是选项，点击Add key。创建一个新的key；
 
 ![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/doc/BigQuery/serviceAccount7.png)
 
 - 8.点击创建，选择JSON格式的秘钥。保存到本地后，打开JSON文件，复制全部内容到Tapdata创建连接页面，将复制到的内容粘贴到服务账号文本域中即可；
 
 ![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/doc/BigQuery/serviceAccount8.png) 
 
获取**数据集ID**

  - 1.进入BigQuery 控制台：https://console.cloud.google.com/bigquery
  
  - 可以从界面，直接获取数据集 ID，如下图所示，依次看到的层级关系为项目ID->数据集ID->数据表ID：
  
  ![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/doc/BigQuery/serviceAccount9.png)


### 3.任务出错自查引导

<a name="errors" id="errors" href="#errors"></a>

1.出错内容包含以下文本时：

```
INVALID_ARGUMENT: Errors found while processing rows. Please refer to the row_errors field for details. The list may not be complete because of the size limitations. Entity:
```

 ***第一***：检查目标表 ‘已有数据处理’ 策略，是否 保持目标表原有结构；

 ***第二***：检查生成的表模型；

 ***第三***：检查BigQuery是否存在这个表，并查看表结构；

 ***第四***：检查任务生成的表模型与BigQuery存在的同名表的表结构是否一致，包括字段及其字段类型；

 ***第五***：如果不一致，为避免新生成的模型与目标端同名表不一致而产生的问题，建议将 ‘已处理数据’ 策略修改为 ‘清除目标端表结构及数据’。
