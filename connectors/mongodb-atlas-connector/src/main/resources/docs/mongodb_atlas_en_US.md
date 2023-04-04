## **Connect configuration help**
###  **1. MongoDB Atlas Installation Instructions**
Follow these instructions to ensure that the MongoDB Atlas database is successfully added and used in Tapdata.
>**Note**：When connecting to MongoDB Atlas, you need to enter a connection string based on the URI demonstration format of the MongoDB Atlas database connection. The connection string must be specified as user name, password, and database name.
#### **2. Supported version**
MongoDB Atlas 5.0.15
>**Note**：Ensure that both the resource database and the target database are of version 5.0 or later.
###  **3. Prerequisites**
#### **3.1 as the source database**
##### **3.1.1 Account Permissions**
If security authentication is enabled on the source MongoDB Atlas, the user account Tapdata uses to connect to the source MongoDB Atlas must have the following built-in roles:
```
readAnyDatabase@admin
```
To create a user with the above rights, see the following:
```
In the menu bar of the Atlas management interface, select Database Access and click "ADD NEW DATABASE USER" button to add users and grant related permissions.
```
##### **3.1.2 Get URL**
After creating a user, you can:
```
From the menu bar, select Database and then click "Connect" --> "Connect your application"
You can get the URL.
```
URL format:
```
mongodb+srv://<username>:<password>@atlascluster.bo1rp4b.mongodb.net/<databaseName>?retryWrites=true&w=majority
```
#### **3.2 as the target database**
#####  **3.2.1 Basic Configuration**
If the target MongoDB Atlas has security authentication enabled, the user account Tapdata uses to connect to the source MongoDB Atlas must have the following built-in roles:
```
readWriteAnyDatabase@admin
```
### **4.  MongoDB Atlas TLS/SSL configuration**
- **Enable TLS/SSL**<br>
  Please select "Yes" in "Connect using TLS/SSL" on the left configuration page to configure<br>
- **Set MongoDB PemKeyFile**<br>
  Click "Select File" and select the certificate file. If the certificate file is password protected, fill in the password in "Private Key Password"<br>
- **Set CAFile**<br>
  Please select "Yes" in "Validate server certificate" on the left configuration page<br>
  Then click "Select File" in "Authentication and Authorization" below<br>