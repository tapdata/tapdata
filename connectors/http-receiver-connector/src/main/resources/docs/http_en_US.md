# HttpReceiverConnector

## 1  Configuration instructions

- Table Name

- Service URL Access Address

- Service token expiration time (in seconds)

- Supplier List

- Data processing script

### 1.1 Table Name

    It is recommended not to use special characters for table names, such as:!@#$%^&*()...

### 1.2 Service URL Access Address

The service URL address is a fixed value, and you can use the service URL to push data to the current data source. The pushed data will be written as incremental data to the table you have configured.

For security reasons, the service URL is filled in with the access key using the PartVariable method, as follows:

    Assuming the service URL is: http://0.0.0.0:8080/api/{access_token}

    Assuming access_token is: 54656fafr465dafas1av1g5d65

    Your complete service URL is: http://0.0.0.0:8080/api/54656fafr465dafas1av1g5d65

Therefore, when pushing data:

If it is the first push, you need to call the ***Service Token Acquisition Link*** to obtain the service token

If the service token has expired, you also need to call the ***Service Token Acquisition Link*** to obtain the service token

Regarding ***Service Token Acquisition Link***, please refer to the instructions below.

### 1.3 Service token expiration time (in seconds)

The expiration time of the service token, which defaults to 3600 seconds, represents the validity period of the service token obtained by using ***Service Token Acquisition Link***. After expiration, ***Service Token Acquisition Link*** will need to be used to retrieve the service token again

### 1.4 Supplier List

#### 1.4.1 Supplier ID

This value needs to be manually entered by the user and used to distinguish suppliers. It can be distinguished in the processing script through the parameter ****supplierId****, for example

    You have added a supplier and set the supplier ID to 'Gavin'. After this supplier pushes data, the value of supplierId in the data processing script will be 'Gavin'

After entering the supplier ID, ***Service Token Acquisition Link*** can be generated for the corresponding supplier

After saving the current configuration, the newly added supplier information will take effect,

The corresponding supplier can use the current ***Service Token Acquisition Link*** to obtain the service token,

After obtaining the service token, it can be concatenated into ***Service Token Acquisition Link*** for data push

#### 1.4.2 Service Token Acquisition Link

The service token acquisition link is used to obtain the service token during the first data push and to retrieve the service token again after the service token expires,

After adding a service provider, the corresponding service token can be generated with one click after entering the provider ID to obtain the link,

The service token acquisition link is a long-term link with a validity period of 99 years. If it is no longer needed, you can manually remove it from list.

### 1.3 Data processing script

    Because each third-party platform's message push system has its own rules, 

    and the pushed data also has its own unique characteristics, 

    you need to use this script flexibly to retrieve the corresponding data according to your needs;
    
    Therefore, data processing scripts are used to process messages pushed by third-party platforms,

    extract the corresponding required data from the messages, and return them according to specified rules.
    
    Such as:

    (1) A certain platform pushed an event through WebHook:

```json
{
  "eventType": "ADD_MESSAGE",
  "time": 1256467862232000,
  "message": {
    "title": "This is sample message",
    "context": "Sample message for everyone.",
    "sender": "Zhang San",
    "to": "Li Si",
    "time": 1256467862232000
  },
  "sender": {
    "name": "Zhang San",
    "id": "12354-5664-2130-45-460",
    "dept": "OpenDept"
  }
}
```

    (2)If the requirement for appeal messages is that we only need the data in the message, the data processing script can be:

```js
function handleEvent(eventData) {
    let eventType = eventData.eventType;
    //Determine the event type and convert it to a unified event type identifier (i: add, u: modify, d: delete)
    let type = "i";
    switch(eventType){
        case "ADD_MESSAGE" : type = "i";break;
        case "UPDATE_MESSAGE": type = "u";break;
        case "DELETE_MESSAGE": type = "d";break;
    }
    return {
        "before": eventData.message, //Data before the event, delete event of type This value is mandatory
        "after": eventData.message,  //The result after the event occurs, this value is mandatory for adding or modifying events of type
        "opType": type,              //The type of event, i: add, u: modify, d: delete
        "time": eventData.time       //The time point at which the event occurred, with a value type of event stamp
    }
}
```

    (3)After the data processing script, you will obtain the following data, and in the task, the following data will be the final data for storage:

```json
{
  "title": "This is sample message",
  "context": "Sample message for everyone.",
  "sender": "Zhang San",
  "to": "Li Si",
  "time": 1256467862232000
}
```

## 2 About trial operation

    1.You need to ensure that you use the service URL configuration here on a third-party platform and have already configured an effective message push service.

    2. Before clicking on the trial run, you can see a certain or specified number of historical messages, and you can use this data for trial run and observe the trial run results;

    3. If you have configured the message push service for the first time on a third-party platform, 
    there may be a situation where you do not have historical message data. 
    In this case, you can go to the third-party platform to trigger the message push by operating the corresponding data on the platform.
    In this case, if the message service configuration is valid, you can obtain the corresponding data from the historical data for trial operation;   
    
    4. Of course, if you are aware of or can obtain the results of message data pushed by third-party platforms through some means, 
    you may wish to manually build the corresponding message data to test run your data processing script, 
    in order to verify whether your data processing script meets your expectations.