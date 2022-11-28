## **Connection Configuration Help**
### **1. Tablestore Installation Instructions**
Please follow the instructions below to ensure successful addition and use of the Elastic Search database in Tapdata.
### **2. Restrictions Instructions**
The current version of the Tapdata system, Tablestore, is only supported as a target.

### **3. Supported Versions**
Tablestore 5.13.9
### **4. Configuration Key**
To access AliCloud's tablestore service, you need to have a valid access key for signature authentication. The following three methods are currently supported.

#### AccessKey ID and AccessKey Secret for AliCloud account. The steps to create one are as follows.
* Register AliCloud account on AliCloud official website.
* Create AccessKey ID and AccessKey Secret.
#### AccessKey ID and AccessKey Secret for RAM users who have been granted access to the table store. Create the steps as follows.
* Go to the Access Control RAM using the AliCloud account and create a new RAM user or use an already existing RAM user.
* Use the AliCloud account to grant the RAM user access to the table storage.
#### Once the RAM user is authorized, they can use their AccessKey ID and AccessKey Secret to access. Temporary access credentials obtained from STS. The steps to obtain them are as follows.
* The application's server obtains a temporary AccessKey ID, AccessKey Secret and SecurityToken by accessing the RAM/STS service and sends it to the using party.
* The using party uses the above temporary key to access the form storage service.

### **5. Cautions**
* It takes a few seconds for the data table to load after it is created, and any read/write operations on the data table will fail during this time. Please wait until the data table is loaded before performing data operations.
* When creating a data table, you must specify the primary key of the data table. The primary key contains 1 to 4 primary key columns, each of which has a name and type.
* Emptying table data is not supported.