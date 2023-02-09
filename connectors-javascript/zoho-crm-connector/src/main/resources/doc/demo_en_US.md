### Written in front
If you are interested, you may want to go to the OpenAPI documents：

- OpenAPI Documentation：[https://www.zoho.com/crm/help/developer/api/overview.html](https://www.zoho.com/crm/help/developer/api/overview.html)

Of course, you can also browse the following to quickly get started with the setup process of ZoHo Data Sources.

---

### 1.Property description

1. Client ID：The Client ID code needs to be manually fetched from ZohoDesk by the user and then copied and pasted here.

2. Client Secret：The Client Secret code needs to be manually fetched from ZohoDesk by the user and then copied and pasted here.

3. Grant Token(Generate Code)：Application generation code needs to be used with the client ID code and the client secret, to get the OpenAPI access key and secret refresh token.

---

### 2.Configuration steps
#### 2.1 Basic configuration

1. Register your Application：
    1.  Go to Zoho Developer Console.[https://api-console.zoho.com.cn/](https://api-console.zoho.com.cn/)

    2. Choose a client type Self Client

   ![](https://www.zohowebstatic.com/sites/default/files/crm/1.-types-of-clients.jpg)

    4. Enter the following details:

       Client Name: The name of your application you want to register with Zoho. 

       Homepage URL: The URL of your web page. 

       Authorized Redirect URIs: This parameter is not necessary for system integration, you can fill in an address yourself.

   ![](https://www.zohowebstatic.com/sites/default/files/crm/api-reg-client2.jpg)
    5. Click CREATE.
    6. You will receive the following credentials:

       Client ID: The consumer key generated from the connected app.

       Client Secret: The consumer secret generated from the connected app.

   ![](https://www.zohowebstatic.com/sites/default/files/crm/api-reg-client3.jpg)

   7. Authorization Request
        1. Go to Zoho Developer Console and log in with your Zoho CRM username and password.[https://api-console.zoho.com/](https://api-console.zoho.com/)
        2. Choose Self Client from the list of client types, and click Create Now.
        3. Click OK in the pop up to enable a self client for your account.
        4. Now, your client ID and client secret are displayed under the Client Secret tab.
      
   ![https://www.zohowebstatic.com/sites/default/files/crm/self-client-id-secret-1.png](https://www.zohowebstatic.com/sites/default/files/crm/self-client-id-secret-1.png)
       
      5. Click the Generate Code tab and enter the required scope separated by commas. Refer to our list of [Scopes](https://www.zoho.com/crm/developer/docs/api/v3/scopes.html), for more details. The system throws an 'Enter a valid scope' error when you enter one or more incorrect scopes.
      6. Select the Time Duration for which the grant token is valid. Please note that after this time, the grant token expires.
      7. Enter a description and click Create.
   
   ![](https://www.zohowebstatic.com/sites/default/files/crm/img7.png)

      8. A pop-up displays the list of portals as shown below. Choose your portal. Further, the pop-up displays the list of environments and different organizations under each environment.
   9. Select the organization in an environment you want to generate the authorization code for, and click Create.
   10. The organization-specific grant token code for the specified scope is displayed. Copy the grant token.

   ![](https://www.zohowebstatic.com/sites/default/files/crm/grant-token-self-client.png)

### 2. Complete connection configuration
   1. Fill in the Client ID, Client Secret and Grant Token (Generated Code) in the connection configuration
   2. Click the [Refresh Token] button to obtain an access token.
   3. Save the connection.

