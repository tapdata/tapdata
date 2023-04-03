## **连接配置帮助**

### **1. 先决条件（作为源）**

#### **1.1 填写团队名称**

在您的Coding链接中也可以直观地获取，比如：**https://team_name.coding.net/** ，那么团队名称就是 team_name。

#### **1.2 点击OAuth2.0 按钮进行访问授权**

填写完您的团队名称后直接点击授权按钮，跳转到授权按钮后点击授权后页面自动返回

### **1.3 选择项目**

#### **1.4 选择增量方式**

- 此时此刻，有Coding支持的Webhook形式，也有普通按时轮询的增量方式。
- 当然，如果您选择了比较节约处理器性能的Webhook模式，那么您就需要前往Coding配置Webhook（点击 ’生成服务 URL‘ 右侧的 ‘生成’ 按钮,您可以看到一行简洁明了的URL，在此，需要您复制前往Coding粘贴到Webhook配置页面的服务URL输入框）

##### **1.3.1 Webhook**

- 此模式需要在创建任务前配置好ServiceHook：
- 配置Webhook的流程如下：

```
 1. 一键生成服务URL，并复制到剪切板
```
![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/doc/coding/generate.PNG)

```
 2. 进入您的团队并选择对应的项目
```
![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/doc/coding/init.PNG)

```
 3. 进入项目设置后，找到开发者选项
```
![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/doc/coding/developer.PNG)

```
 4. 找到ServerHook，再找到右上角点的新建ServerHook按钮并点击
```
![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/doc/coding/init-webhook.PNG)

```
 5. 进入Webhook配置，第一步我们选择Http Webhook后点击下一步
```
![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/doc/coding/webhook.PNG)

```
 6. 配置我们需要的监听的事件类型
```
![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/doc/coding/monitor.PNG)

```
 7. 粘贴我们最开始在创建数据源页面生成的服务URL到此
```
![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/doc/coding/url.PNG)


- 一键前往配置Webhook：https://tapdata.coding.net/p/testissue/setting/webhook

##### **1.3.2 轮询式**
...

---

特别说明：**创建新的Coding连接，如果选择Webhook模式，一定要记得前往Coding为此连接节点配置ServiceHook哦！**

---

### **2. 数据说明**

支持增量轮询的任何表在执行增量轮询时都无法监听并处理删除事件（所有修改事件都以插入事件处理），如需要具体的事件区分请选择Webhook增量方式（局限于SaaS平台，并不是所有表都支持Webhook增量）

#### **2.1 事项表-Issues**

事项表包含全部类型包括需求、迭代、任务、史诗以及自定义的类型。
其增量方式在轮询式下无法准确知道增量事件，统一作为新增事件处理。

#### **2.2 迭代表-Iterations**

迭代表包含所有迭代。
受限于Coding的OpenAPI，其轮询式增量采取从头覆盖，意味着任务开始后监控上显示的增量事件数存在误差，但不会造成真实数据误差。

#### **2.3 项目成员表-ProjectMembers**

此表包含当前选中的项目下的全部项目成员。

### 注意事项

- 请不要使用同一个OAuth授权过多数据源，也不要使用同一个Coding数据源为同一张表创建过多的任务，这样可能造成Coding启动限流措施。

