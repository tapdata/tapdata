## **Salesforce Connection Configuration Help**

### 1. Fill in the connection name (required)

This is a required field, and you can customize your connection name.

### 2. Click Grant token

After clicking the "Authorize" button, you will be redirected to the Salesforce login interface. After entering the account password and logging in successfully, you will be redirected to this page, and the authentication is successful.
After the authorization succeeds, you can test the connection and save the connection.

### 3、Other details

If you fail to pass the connection test, the Error message "Error: This feature is not currently enabled for this user. Code: FUNCTIONALITY_NOT_ENABLED HttpCode: 403 ". The Salesforce version you are using may not be one of: Enterprise, Wireless, developer, Professional, and will not be able to synchronize data with Salesforce in Tapdata.
For details, please refer to Salesforce documentation:

```
https://help.salesforce.com/s/articleView?id=000385436&type=1
```