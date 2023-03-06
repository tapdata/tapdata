## **Quick Api Connection Configuration Help**


### 1. Fill in the connection name (required)


The first step, but not necessarily the first step, can also be the last step. You need to fill in the connection name, because this is the first required item.


### 2. Enter the JSON format API text exported from PostMan (required)


The exported JSON file will contain four main parts: info, item, event, and variable;


#### 2.1 info indicates the basic information of the postman API document.


#### 2.2 Item indicates the API interface information contained in the postman API document. You need to ensure that you have certain editing operations for the API interface to be used:


##### 2.2.1 Table interface statement (necessary operation)


You need to add some standardized labels to this API name on the corresponding table data API. For example, if I want to get the work order tickets of the portal on ZoHo Desk, I need to edit the API for getting the work order on PostMan. After processing, the API name should be

```
    TAP_TABLE[Tickets](PAGE_LIMIT: data) Get Work Order List, 
```
which contains the following keywords:


- A、 TAP_ TABLE: the table creation keyword, which indicates that the data obtained by the current API will form a data table.


- B. \[Tickets\]: Specify the table name, generally the same as TAP_ The TABLE keyword appears together, specifying the table name after the table is created and the data obtained by the API is stored in this table. A text wrapped with []. Please organize the table name reasonably. It is not recommended to use special characters. For example, using one of the two characters [] in the table name will affect the table name after the table is created.


- C. (PAGE_LIMIT: data) The LIMIT paging type queries the data, indicating that the API is paging based on the record index and intra page offset. The specific paging type needs to be indicated after you analyze the API interface, otherwise it will affect the query results and cause data errors. The following page types are provided. You can specify the page types according to the relevant API features:


```

    PAGE_SIZE_PAGE_INDEX: Applicable to pagination using page numbers and intra page offsets. Need to match TAP_PAGE_SIZE and TAP_PAGE_INDEX the tag specifies paging parameters.

    FROM_TO: It is applicable to pagination using record start index and end index. Single required TAP_PAGE_FROM and TAP_PAGE_TO the label specifies paging parameters.

    PAGE_LIMIT: Applicable to paging by using record index and intra page offset. Need to match TAP_PAGE_OFFSET and TAP_PAGE_LIMIT The tag specifies paging parameters.

    PAGE_TOKEN: It is applicable to paging using cached paging tokens. The first page is empty, and the next page is queried using the token returned from the last query. Use together TAP_PAGE_TOKEN The tag specifies paging parameters, while using TAP_PAGE_SIZE specifies the number of records per paging query, use TAP_HAS_MORE_PAGE to describe whether there is a field name on the next page (you need to specify this parameter in the parameter list and add this label in the parameter description).

    PAGE_NONE: It is applicable to the general data acquisition of non pagination returned from the list.

```


- D. Paging parameter specification: Take the current query of ZoHo Desk work order API as an example, and the paging type used is PAGE_LIMIT, the paging parameter needs to add a corresponding parameter label in its corresponding description text to indicate TAP_PAGE_OFFSET and TAP_PAGE_LIMIT ,
```
    TAP_PAGE_OFFSET corresponds to interface parameters from, 
    TAP_PAGE_LIMIT The interface parameter corresponding to is limit
```
![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/quickAPI/doc/TAP_TABLE.PNG)

**Supplementary note:**The above is a case of ZoHo Desk work order interface declaration, and the case of API name declaration of coding acquisition is:

TAP_TABLE [Issues] (PAGE_SIZE_PAGE_INDEX:Response.Data.List) Get the list of events

Its semantics are as follows: the event table name is set as Issues, and PAGE is used_ SIZE_ PAGE_ INDEX, the paging logic, specifies the data of Response. Data. List in the API result as the table data.

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/quickAPI/doc/TAP_TABLE-2.PNG)

#####2.2.2 Statement of login authorization interface

You need to use TAP_GET_TOKEN, the tag declares the login interface. It is consistent with the declaration method of the table data interface. A declaration label needs to be added to the interface name. The keyword of the declaration label of the login interface is TAP_GET_TOKEN, this label indicates that the data source will have access_token when calling the API to obtain data to judge , you need to describe the expiration status and specify access on the connection configuration page_ The key value obtained by token matches., For example, the following figure shows the statement of the login interface for ZoHo Desk in Postman:

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/quickAPI/doc/TAP_LOGIN.PNG)

####2.3 Event indicates some Postman events, which we can hardly use.


####2.4 variable refers to some variables defined in the interface. It is necessary to ensure that the variables defined in the API can be found in this variable and have actual and correct values. Otherwise, the API interface that uses variables with missing or incorrect values will fail in the near future.


###3. Fill in access_token expiration status description (optional)


Note: 

The reason why this entry is optional is that the OpenAPI provided by some Saas platforms uses a permanent access token, regardless of the expiration of the token, such as Coding. However, for Saas platforms that use temporary tokens to access OpenAPI, you need to fill in this entry, otherwise unpredictable consequences may result.

Fill in access_token expiration status description. (The access_token here generally refers to the API interface access token. The names of each Saas may be different.)


- 3.1 access_token expiration status refers to the access failure status returned by the Saas platform after calling the specified interface after your API access expires.

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/quickAPI/doc/TAP_TABLE-ZoHo.PNG)

For example, when we call ZoHo to obtain a work order, access_token has expired, and the returned result is shown in the following figure. Then you can describe the expiration status as errorCode=INVALID_OAUTH, so that when the API is executed again, it can automatically recognize the expired token according to the returned results to refresh the token automatically.


- 3.2 This status description needs to be summarized manually through the PostMan access API (because we cannot predict what response results these Saas platforms will return after the access_token expires);


- 3.3 Declare the API interface for login (access to API) in PostMan. When the API is executed, access is found the token expires and regrets calling the specified API for access_token refresh. This login interface needs to add to the name of the interface TAP_LOGIN is a symbolic text. For example, the name of the ZoHo token refresh interface is "TAP_LOGIN refresh AccessToken login", which adds TAP_LOGIN (see the upper left corner) indicates that this interface is used to implement automatic token refresh operations.

![](https://tapdata-bucket-01.oss-cn-beijing.aliyuncs.com/quickAPI/doc/TAP_LOGIN-ZoHo.PNG)

- 3.4 Expiration status description has the following description rules:

```properties
// Support direct value assignment 
body.errorCode=NO AUTH

// Support and relationship judgment，split with &&
body.errorCode=NO AUTH&&body.code=500021

//Support or relationship judgment, Line breaking 
body.code=500021
body.code=500021

// Support range value 
body.code=[50000,51000]

// Consider supporting regular expressions 
body.errorCode=regex('$/[0-1|a-z]{5}/$')

header.httpCode=401

code=401
```

### 4. Specify the key value matching rules after automatically refreshing the token (optional)


Note:


As an optional reason, you need to fill in this entry after configuring the token status description, otherwise unpredictable consequences may occur,


Although the system will fuzzy match the key data in the return value of the login interface to the global parameter list, it cannot guarantee that the token on the fuzzy match can be correctly assigned to the user-defined token parameter.


Because the fuzzy matching rule is only an empirical value, it cannot guarantee 100% successful matching. The general matching idea is as follows:


(1) Find out the possible token fields and their corresponding values in the return value of the login authorization interface.


```

According to the keyword access_ Token, find the qualified token in the interface return value;

If the first step is not in progress, use the token keyword to search. If there are multiple such values, find the one closest to the format of the access token value in the global parameter as the access token;

If it is still not found, use the token keyword to perform the previous operation.

Finally, if there is no way to find out, an error will be thrown and a prompt will be generated. You need to manually specify the return result and the token key value rule in the global variable.

```

(2) Find out the attribute that may be the access token in the global parameter and reassign it.

Find out the variables used by the Authorization parameter in the interface headers in the global parameter list.



It is necessary to indicate which key value of the obtained result from the API refreshing the token corresponds to the AccessToken described in the document.


For example:

```

I used Postman to export the interface collection in ZoHo Desk,

I used a global parameter accessToken to declare a global variable,

This variable applies to all APIs and is used as the access token of the API.


The access token name returned by the login interface of zoho desk is called access_ tokon，

At this point, we need to declare here that accessToken=access_ token 。

```


###Data source support


-1. API declaration in PostMan, which includes at least one API declared by TAP_TABLE. Otherwise, the connection created by this will not be scanned to any table. TAP_TABLE needs to declare the table name, page type, and page parameter at the same time. Otherwise, incorrect results will occur.


-2. You may need to configure the login authorization API. If you have configured the login authorization API, you need to configure the token expiration rule on the connection configuration page and specify the corresponding relationship between the returned result and the token variable in the global variable in the interface to obtain the token.


-3. Support Saas data sources in most scenarios, such as:


The permanent token is used to make OpenAPI calls, such as Coding.


ZoHo Desk is called by using the OpenAPI of dynamically refreshing access tokens.