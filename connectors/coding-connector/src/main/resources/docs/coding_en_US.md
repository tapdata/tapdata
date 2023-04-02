## **Connection Configuration Help**

### **1. Prerequisites (As source)**

#### **1.1 Fill in the team name**

It can also be obtained intuitively in your coding link, such as: **https://team_name.coding.net/**, then team name is team_name.

#### **1.2 Obtain the access_token from the coding management page**

After filling in your team name, directly click the authorization button, jump to the authorization button, and click the authorization button to automatically return to the page

#### **1.3 Select Incremental Method**

- At this moment, there are web hooks supported by Coding, and there are also incremental methods of regular polling on time.
- Of course, if you choose the WebHook mode that saves processor performance, you need to go to Coding to configure the web hook (click the "Generate" button on the right side of "Generate Service URL". Here, You are required to copy and paste the service URL input box from Coding to the WebHook configuration page)

##### **1.3.1 WebHook**

- In this mode, you need to configure ServiceHook before creating tasks:
- The process of configuring web hooks is as follows:

```
 1. Generate a service URL with one click and copy it to the clipboard
```
![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/doc/coding/generate.PNG)

```
 2. Enter your team and select the corresponding project
```
![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/doc/coding/init.PNG)

```
 3. After entering the project settings, find the developer options
```
![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/doc/coding/developer.PNG)

```
 4. Locate the ServerHook, locate the New ServerHook button in the upper right corner, and click
```
![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/doc/coding/init-webhook.PNG)

```
 5. Enter the Webhook configuration. The first step is to select Http Webhook and click Next
```
![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/doc/coding/webhook.PNG)

```
 6. Configure the event types we need to listen to
```
![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/doc/coding/monitor.PNG)

```
 7. Paste the service URL we first generated on the Create Data Source page here
```
![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/doc/coding/url.PNG)


- One click to configure WebHook：https://tapdata.coding.net/p/testissue/setting/webhook

##### **1.3.2 polled**

...

---

 Special instructions: **Create a new coding connection. If you choose WebHook mode, remember to go to Coding to configure ServiceHook for this connection node**

---

### **2. Data description**

Any table that supports incremental polling cannot listen to and handle deletion events when executing incremental polling (all modification events are handled as insert events). If specific event differentiation is required, please select the WebHook incremental mode (limited to the SaaS platform, not all tables support webHook incremental)

#### **2.1 Issue table-Issues**

- The list contains all types including requirements, iterations, tasks, epics, and custom types.
- The incremental event cannot be known accurately in the polling mode, so it is uniformly treated as a new event.

#### **2.2 Iteration table-Iterations**

- Iterations represent all iterations. 
- Limited by the OpenAPI of Coding, its polling type increment is overwritten from the beginning, which means that there is an error in the number of incremental events displayed on the monitoring after the task starts, but it will not cause real data errors.

#### **2.3 Project members table-ProjectMembers**

This table contains all project members under the currently selected project.