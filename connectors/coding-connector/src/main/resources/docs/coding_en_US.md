## **Connection Configuration Help**
### **1. Prerequisites (As source)**
#### **1.1 Fill in the team name**
It can also be obtained intuitively in your coding link, such as：https://{teamName}.coding.net/,Then his team name is teamName
#### **1.2 Obtain the access token token from the coding management page**
After filling in your team name, directly click the authorization button, jump to the authorization button, and click the authorization button to automatically return to the page
#### **1.3 Select Incremental Method**
- At this moment, there are web hooks supported by Coding, and there are also incremental methods of regular polling on time.
- Of course, if you choose the WebHook mode that saves processor performance, you need to go to Coding to configure the web hook (click the HookButton and you can see a line of concise URLs. Here, you need to copy the URL input box that goes to Coding and pastes to the WebHook configuration page.)
##### **1.3.1 polled**
##### **1.3.1 WebHook**
- In this mode, you need to configure ServiceHook before creating tasks:
- The process of configuring web hooks is as follows:
```
    Enter your team ->
    Select an item ->
    Enter project settings ->
    Click Developer Options ->
    Click ServerHook ->
    Click Create ServerHook in the upper right corner ->
    Select HttpWebHook and click Next ->
    Select the trigger event you want and click Next ->
    At this time, set the configuration according to your needs, and paste the previously generated URL into the service URL.
```
- One click to configure WebHook：https://tapdata.coding.net/p/testissue/setting/webhook

---
 Special instructions ：**Create a new coding connection. If you choose WebHook mode, remember to go to Coding to configure ServiceHook for this connection node!**
---

### **3. Prerequisites (As goals)**
...

### **4. Data description**
Any table that supports incremental polling cannot listen to and handle deletion events when executing incremental polling (all modification events are handled as insert events). If specific event differentiation is required, please select the WebHook incremental mode (limited to the SaaS platform, not all tables support webHook incremental)
#### **4.1 Issue table-Issues**
- The list contains all types including requirements, iterations, tasks, epics, and custom types.
- The incremental event cannot be known accurately in the polling mode, so it is uniformly treated as a new event.

#### **4.2 Iteration table-Iterations**
- Iterations represent all iterations. 
- Limited by the OpenAPI of Coding, its polling type increment is overwritten from the beginning, which means that there is an error in the number of incremental events displayed on the monitoring after the task starts, but it will not cause real data errors.

#### **4.3 Project members table-ProjectMembers**
This table contains all project members under the currently selected project.