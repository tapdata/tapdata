config.setStreamReadIntervalSeconds(10);
/**
 * @return The returned result cannot be empty and must conform to one of the following forms:
 *      (1)Form one:  A string (representing only one table name)
 *          return 'example_table_name';
 *      (2)Form two: A String Array (It means that multiple tables are returned without description, only table name is provided)
 *          return ['example_table_1', 'example_table_2', ... , 'example_table_n'];
 *      (3)Form three: A Object Array ( The complex description of the table can be used to customize the properties of the table )
 *          return [
 *           {
 *               "name: '${example_table_1}',
 *               "fields": {
 *                   "${example_field_name_1}":{
 *                       "type":"${field_type}",
 *                       "default:"${default_value}"
 *                   },
 *                   "${example_field_name_2}":{
 *                       "type":"${field_type}",
 *                       "default:"${default_value}"
 *                   }
 *               }
 *           },
 *           '${example_table_2}',
 *           ['${example_table_3}', '${example_table_4}', ...]
 *          ];
 * @param connectionConfig  Configuration property information of the connection page
 * */
function discoverSchema(connectionConfig) {
    //return ['Leads','Contacts','Accounts','Potentials','Quotes'];
    return [{
        "name":'Leads',
        "fields": {
            "Owner": {
                "type": "Object",
                "comment": "线索所有者的名称和ID",
                "nullable": false
            },
            "Company": {
                "type": "String",
                "comment": "线索工作的公司名称。这个字段是必填的",
                "nullable": false
            },
            "Email": {
                "type": "String",
                "comment": "线索的邮件地址",
                "nullable": false
            },
            "_currency_symbol": {
                "type": "String",
                "comment": "产生收入的货币",
                "nullable": false
            },
            "Visitor_Score": {
                "type": "String",
                "comment": "从SalesIQ获得的线索分数",
                "nullable": true
            },
            "Last_Activity_Time": {
                "type": "String",
                "comment": "记录最后一次在操作中使用的日期和时间。这是一个系统生成的字段。你不能修改它。",
                "nullable": false
            },
            "Industry": {
                "type": "String",
                "comment": "代表线索所属的行业",
                "nullable": false
            },
            "_converted": {
                "type": "Boolean",
                "comment": "表示是否将线索转化为商机或联系人",
                "nullable": false
            },
            "_process_flow": {
                "type": "Boolean",
                "comment": "表示记录是否为蓝图数据",
                "nullable": false
            },
            "Street": {
                "type": "String",
                "comment": "线索的街道地址",
                "nullable": false
            },
            "Zip_Code": {
                "type": "String",
                "comment": "",
                "nullable": false
            },
            "id": {
                "type": "String",
                "comment": "线索的唯一ID",
                "nullable": false,
                'isPrimaryKey':true,
                'primaryKeyPos':1
            },
            "_approved": {
                "type": "Boolean",
                "comment": "表示记录是否被批准",
                "nullable": false
            },
            "_approval": {
                "type": "Object",
                "comment": "表示当前用户是否可以批准、委托、拒绝或重新提交在此记录上执行的操作",
                "nullable": false
            },
            "First_Visited_URL": {
                "type": "String",
                "comment": "线索首先访问的页面URL地址。这是一个系统生成的字段。你不能修改它",
                "nullable": true
            },
            "Days_Visited": {
                "type": "String",
                "comment": "线索访问Zoho CRM的天数",
                "nullable": true
            },
            "Created_Time": {
                "type": "String",
                "comment": "记录创建的日期和时间。这是一个系统生成的字段。你不能修改它",
                "nullable": false
            },
            "_editable": {
                "type": "Boolean",
                "comment": "表示记录是否可编辑",
                "nullable": false
            },
            "City": {
                "type": "String",
                "comment": "",
                "nullable": false
            },
            "No_of_Employees": {
                "type": "Number",
                "comment": "线索公司的员工人数",
                "nullable": false
            },
            "State": {
                "type": "String",
                "comment": "",
                "nullable": false
            },
            "Country": {
                "type": "String",
                "comment": "",
                "nullable": false
            },
            "Last_Visited_Time": {
                "type": "String",
                "comment": "线索最后一次访问Zoho CRM的日期和时间。这是一个系统生成的字段。你不能修改它",
                "nullable": true
            },
            "Created_By": {
                "type": "Object",
                "comment": "创建记录的用户名称和ID。这是一个系统生成的字段。你不能修改它",
                "nullable": false
            },
            "Annual_Revenue": {
                "type": "Number",
                "comment": "线索公司的年收入",
                "nullable": false
            },
            "Secondary_Email": {
                "type": "String",
                "comment": "线索的另一个邮件地址",
                "nullable": true
            },
            "Description": {
                "type": "String",
                "comment": "线索说明",
                "nullable": true
            },
            "Number_Of_Chats": {
                "type": "Number",
                "comment": "线索与Zoho CRM团队交谈的次数。这是一个系统生成的字段。你不能修改它。",
                "nullable": true
            },
            "Rating": {
                "type": "String",
                "comment": "表示线索的评级",
                "nullable": true
            },
            "Website": {
                "type": "String",
                "comment": "线索的网站",
                "nullable": false
            },
            "Twitter": {
                "type": "String",
                "comment": "线索的Twitter",
                "nullable": false
            },
            "Average_Time_Spent_Minutes": {
                "type": "Number",
                "comment": "线索在Zoho CRM中的平均时间(以分钟为单位)。这是一个系统生成的字段。你不能修改它",
                "nullable": true
            },
            "Salutation": {
                "type": "String",
                "comment": "线索的称呼",
                "nullable": false
            },
            "First_Name": {
                "type": "String",
                "comment": "线索的名",
                "nullable": false
            },
            "Lead_Status": {
                "type": "String",
                "comment": "线索状态",
                "nullable": false
            },
            "Full_Name": {
                "type": "String",
                "comment": "线索的全名",
                "nullable": false
            },
            "Record_Image": {
                "type": "String",
                "comment": "线索的头像",
                "nullable": true
            },
            "Modified_By": {
                "type": "Object",
                "comment": "修改线索的用户名称和ID。这是一个系统生成的字段。你不能修改它",
                "nullable": false
            },
            "Skype_ID": {
                "type": "String",
                "comment": "线索的Skype ID",
                "nullable": false
            },
            "Phone": {
                "type": "String",
                "comment": "线索的电话号码",
                "nullable": false
            },
            "Email_Opt_Out": {
                "type": "Boolean",
                "comment": "指定线索是否选择不接受来自Zoho CRM的电子邮件通知",
                "nullable": false
            },
            "Designation": {
                "type": "String",
                "comment": "线索在公司的职位",
                "nullable": false
            },
            "Modified_Time": {
                "type": "String",
                "comment": "记录最后一次修改的日期和时间。这是一个系统生成的字段。你不能修改它",
                "nullable": false
            },
            "_converted_detail": {
                "type": "Object",
                "comment": "将线索转换到的模块名称和ID",
                "nullable": false
            },
            "Mobile": {
                "type": "String",
                "comment": "线索的手机号",
                "nullable": false
            },
            "Prediction_Score": {
                "type": "String",
                "comment": "线索的预测得分。这是一个系统生成的字段。你不能修改它",
                "nullable": true
            },
            "First_Visited_Time": {
                "type": "String",
                "comment": "线索首次访问Zoho CRM的日期和时间。这是一个系统生成的字段。你不能修改它",
                "nullable": true
            },
            "Last_Name": {
                "type": "String",
                "comment": "线索的姓。这是一个必填字段",
                "nullable": false
            },
            "Referrer": {
                "type": "String",
                "comment": "推荐这条线索的联系人姓名和ID",
                "nullable": true
            },
            "Lead_Source": {
                "type": "String",
                "comment": "表示创建线索的来源",
                "nullable": false
            },
            "Tag": {
                "type": "Array",
                "comment": "与记录关联的标记列表",
                "nullable": false
            },
            "Fax": {
                "type": "String",
                "comment": "线索的传真号",
                "nullable": true
            }
        }
    },{
        "name":'Contacts',
        "fields":{
            "Owner": {
                "type": "Object",
                "comment": "客户所有者的姓名和ID",
                "nullable": false
            },
            "Email": {
                "type": "String",
                "comment": "联系人的邮件地址",
                "nullable": false
            },
            "_currency_symbol": {
                "type": "String",
                "comment": "产生收入的货币",
                "nullable": false
            },
            "Visitor_Score": {
                "type": "String",
                "comment": "从SalesIQ获得的联系人得分",
                "nullable": true
            },
            "Other_Phone": {
                "type": "String",
                "comment": "联系人的其他电话号码(如果有的话)",
                "nullable": true
            },
            "Mailing_State": {
                "type": "String",
                "comment": "联系人的主要通讯地址",
                "nullable": false
            },
            "Other_State": {
                "type": "String",
                "comment": "联系人的其他地址(如有)",
                "nullable": true
            },
            "Other_Country": {
                "type": "String",
                "comment": "",
                "nullable": true
            },
            "Last_Activity_Time": {
                "type": "String",
                "comment": "记录最后一次在操作中使用的日期和时间。这是一个系统生成的字段。你不能修改它。",
                "nullable": false
            },
            "Department": {
                "type": "String",
                "comment": "表示联系人所在部门",
                "nullable": true
            },
            "_process_flow": {
                "type": "Boolean",
                "comment": "表示记录是否为蓝图数据",
                "nullable": false
            },
            "Assistant": {
                "type": "String",
                "comment": "联系人助理的姓名",
                "nullable": true
            },
            "Mailing_Country": {
                "type": "String",
                "comment": "",
                "nullable": false
            },
            "id": {
                "type": "String",
                "comment": "联系人的唯一ID",
                "nullable": false,
                'isPrimaryKey':true,
                'primaryKeyPos':1
            },
            "_approved": {
                "type": "Boolean",
                "comment": "表示记录是否被批准",
                "nullable": false
            },
            "Reporting_To": {
                "type": "String",
                "comment": "此联系人向其报告的联系人名称和ID",
                "nullable": true
            },
            "_approval": {
                "type": "Object",
                "comment": "表示当前用户是否可以批准、委托、拒绝或重新提交在此记录上执行的操作",
                "nullable": false
            },
            "First_Visited_URL": {
                "type": "String",
                "comment": "联系人首先访问的页面URL地址。这是一个系统生成的字段。你不能修改它",
                "nullable": true
            },
            "Days_Visited": {
                "type": "String",
                "comment": "联系人访问Zoho CRM的天数",
                "nullable": true
            },
            "Other_City": {
                "type": "String",
                "comment": "",
                "nullable": true
            },
            "Created_Time": {
                "type": "String",
                "comment": "记录创建的日期和时间。这是一个系统生成的字段。你不能修改它",
                "nullable": false
            },
            "_editable": {
                "type": "Boolean",
                "comment": "表示记录是否可编辑",
                "nullable": false
            },
            "Home_Phone": {
                "type": "String",
                "comment": "联系人的家庭电话",
                "nullable": true
            },
            "Last_Visited_Time": {
                "type": "String",
                "comment": "联系人上次访问Zoho CRM的日期和时间。这是一个系统生成的字段。你不能修改它",
                "nullable": true
            },
            "Created_By": {
                "type": "Object",
                "comment": "创建记录的用户名称和ID。这是一个系统生成的字段。你不能修改它",
                "nullable": false
            },
            "Secondary_Email": {
                "type": "String",
                "comment": "联系人的另一个电子邮件地址",
                "nullable": true
            },
            "Description": {
                "type": "String",
                "comment": "联系人说明",
                "nullable": true
            },
            "Vendor_Name": {
                "type": "String",
                "comment": "与联系人相关的供应商名称和ID",
                "nullable": true
            },
            "Mailing_Zip": {
                "type": "String",
                "comment": "",
                "nullable": false
            },
            "Number_Of_Chats": {
                "type": "Number",
                "comment": "联系人与Zoho CRM团队进行交谈的次数。这是一个系统生成的字段。你不能修改它。",
                "nullable": false
            },
            "Twitter": {
                "type": "String",
                "comment": "联系人的Twitter",
                "nullable": false
            },
            "Other_Zip": {
                "type": "String",
                "comment": "",
                "nullable": true
            },
            "Mailing_Street": {
                "type": "String",
                "comment": "",
                "nullable": false
            },
            "Average_Time_Spent_Minutes": {
                "type": "String",
                "comment": "联系人在Zoho CRM中花费的平均时间(以分钟为单位)。它是一个系统生成的字段。你不能修改它",
                "nullable": true
            },
            "Salutation": {
                "type": "String",
                "comment": "",
                "nullable": false
            },
            "First_Name": {
                "type": "String",
                "comment": "先生、女士、夫人或其他人",
                "nullable": false
            },
            "Asst_Phone": {
                "type": "String",
                "comment": "联系人助理的电话号码(如果有的话)",
                "nullable": true
            },
            "Full_Name": {
                "type": "String",
                "comment": "联系人的全名",
                "nullable": false
            },
            "Record_Image": {
                "type": "String",
                "comment": "联系人的角色头像",
                "nullable": true
            },
            "Modified_By": {
                "type": "Object",
                "comment": "修改客户的用户名称和ID。这是一个系统生成的字段。你不能修改它",
                "nullable": false
            },
            "Skype_ID": {
                "type": "String",
                "comment": "联系人的Skype ID",
                "nullable": false
            },
            "Phone": {
                "type": "String",
                "comment": "联系人的电话号码",
                "nullable": false
            },
            "Account_Name": {
                "type": "Object",
                "comment": "与联系人关联的客户名称和ID",
                "nullable": false
            },
            "Email_Opt_Out": {
                "type": "Boolean",
                "comment": "指定联系人是否选择退出Zoho CRM的电子邮件通知",
                "nullable": false
            },
            "Modified_Time": {
                "type": "String",
                "comment": "记录最后一次修改的日期和时间。这是一个系统生成的字段。你不能修改它",
                "nullable": false
            },
            "Date_of_Birth": {
                "type": "String",
                "comment": "联系人出生日期格式： 月/日/年",
                "nullable": true
            },
            "Mailing_City": {
                "type": "String",
                "comment": "Hamilton",
                "nullable": false
            },
            "Title": {
                "type": "String",
                "comment": "联系人的职位/职务",
                "nullable": false
            },
            "Other_Street": {
                "type": "String",
                "comment": "",
                "nullable": true
            },
            "Mobile": {
                "type": "String",
                "comment": "联系人的手机号码",
                "nullable": false
            },
            "First_Visited_Time": {
                "type": "String",
                "comment": "联系人首次访问Zoho CRM的日期和时间。这是一个系统生成的字段。你不能修改它",
                "nullable": true
            },
            "Last_Name": {
                "type": "String",
                "comment": "联系人的姓。这是一个必填字段。",
                "nullable": false
            },
            "Referrer": {
                "type": "String",
                "comment": "推荐此联系人的联系人姓名和ID",
                "nullable": true
            },
            "Lead_Source": {
                "type": "String",
                "comment": "表示创建联系人的来源",
                "nullable": false
            },
            "Tag": {
                "type": "Array",
                "comment": "与记录关联的标记列表",
                "nullable": false
            },
            "Fax": {
                "type": "String",
                "comment": "联系人的传真号码",
                "nullable": true
            }
        }
    }, {
        "name":'Accounts',
        "fields":{
            "Owner": {
                "type": "Object",
                "comment": "客户所有者的名称和ID",
                "nullable": false
            },
            "Ownership": {
                "type": "String",
                "comment": "表示公司所有权的类型",
                "nullable": false
            },
            "Description": {
                "type": "String",
                "comment": "客户说明",
                "nullable": true
            },
            "_currency_symbol": {
                "type": "String",
                "comment": "产生收入的货币",
                "nullable": false
            },
            "Account_Type": {
                "type": "String",
                "comment": "表示客户类型",
                "nullable": true
            },
            "Rating": {
                "type": "String",
                "comment": "表示客户评级",
                "nullable": true
            },
            "SIC_Code": {
                "type": "Number",
                "comment": "四位数SIC(标准行业分类)代码为行业类型的客户",
                "nullable": false
            },
            "Shipping_State": {
                "type": "String",
                "comment": "发货客户的发货地址",
                "nullable": true
            },
            "Website": {
                "type": "String",
                "comment": "公司网站的网址",
                "nullable": false
            },
            "Employees": {
                "type": "Number",
                "comment": "公司员工人数",
                "nullable": false
            },
            "Last_Activity_Time": {
                "type": "String",
                "comment": "记录最后一次在操作中使用的日期和时间",
                "nullable": false
            },
            "Industry": {
                "type": "String",
                "comment": "客户所在行业的名称",
                "nullable": false
            },
            "Record_Image": {
                "type": "String",
                "comment": "客户的角色头像",
                "nullable": true
            },
            "Modified_By": {
                "type": "Object",
                "comment": "修改客户的用户名称和ID。这是一个系统生成的字段。你不能修改它",
                "nullable": false
            },
            "Account_Site": {
                "type": "String",
                "comment": "客户位置的名称，例如，总部或伦敦。",
                "nullable": true
            },
            "_process_flow": {
                "type": "Boolean",
                "comment": "表示记录是否为蓝图数据",
                "nullable": false
            },
            "Phone": {
                "type": "String",
                "comment": "客户的电话号码",
                "nullable": false
            },
            "Billing_Country": {
                "type": "String",
                "comment": "客户的帐单地址，以发送报价、发货单等协议",
                "nullable": false
            },
            "Account_Name": {
                "type": "String",
                "comment": "客户名称。这是一个必填字段",
                "nullable": false
            },
            "id": {
                "type": "String",
                "comment": "客户的唯一ID",
                "nullable": false,
                'isPrimaryKey':true,
                'primaryKeyPos':1
            },
            "Account_Number": {
                "type": "String",
                "comment": "客户的的参考编号",
                "nullable": false
            },
            "_approved": {
                "type": "Boolean",
                "comment": "表示记录是否被批准",
                "nullable": false
            },
            "Ticker_Symbol": {
                "type": "String",
                "comment": "表示客户的股票代码",
                "nullable": false
            },
            "_approval": {
                "type": "String",
                "comment": "表示当前用户是否可以批准、委托、拒绝或重新提交在此记录上执行的操作",
                "nullable": false
            },
            "Modified_Time": {
                "type": "String",
                "comment": "记录最后一次修改的日期和时间。这是一个系统生成的字段。你不能修改它",
                "nullable": false
            },
            "Billing_Street": {
                "type": "String",
                "comment": "",
                "nullable": false
            },
            "Created_Time": {
                "type": "String",
                "comment": "记录创建的日期和时间。这是一个系统生成的字段。你不能修改它",
                "nullable": false
            },
            "_editable": {
                "type": "Boolean",
                "comment": "表示记录是否可编辑",
                "nullable": false
            },
            "Billing_Code": {
                "type": "String",
                "comment": "",
                "nullable": false
            },
            "Parent_Account": {
                "type": "String",
                "comment": "父客户的名称和ID",
                "nullable": true
            },
            "Shipping_City": {
                "type": "String",
                "comment": "",
                "nullable": true
            },
            "Shipping_Country": {
                "type": "String",
                "comment": "",
                "nullable": true
            },
            "Shipping_Code": {
                "type": "String",
                "comment": "",
                "nullable": true
            },
            "Billing_City": {
                "type": "String",
                "comment": "",
                "nullable": false
            },
            "Billing_State": {
                "type": "String",
                "comment": "",
                "nullable": false
            },
            "Tag": {
                "type": "Array",
                "comment": "与记录关联的标记列表",
                "nullable": false
            },
            "Created_By": {
                "type": "Object",
                "comment": "创建记录的用户名称和ID。这是一个系统生成的字段。你不能修改它",
                "nullable": false
            },
            "Fax": {
                "type": "String",
                "comment": "客户的传真号码",
                "nullable": true
            },
            "Annual_Revenue": {
                "type": "Number",
                "comment": "客户的年收入",
                "nullable": false
            },
            "Shipping_Street": {
                "type": "String",
                "comment": "",
                "nullable": true
            }
        }
    }, {
        "name":"Potentials",
        "fields":{
            "Owner": {
                "type": "Object",
                "comment": "商机所有者的姓名和ID",
                "nullable": false
            },
            "Description": {
                "type": "String",
                "comment": "服务支持说明",
                "nullable": true
            },
            "_currency_symbol": {
                "type": "String",
                "comment": "产生收入的货币",
                "nullable": false
            },
            "Campaign_Source": {
                "type": "String",
                "comment": "代表与该商机相关的市场活动",
                "nullable": true
            },
            "_followers": {
                "type": "String",
                "comment": "表示该商机的跟进者数量",
                "nullable": true
            },
            "Closing_Date": {
                "type": "String",
                "comment": "交易完成的日期。这是一个必填字段。",
                "nullable": false
            },
            "Last_Activity_Time": {
                "type": "String",
                "comment": "记录最后一次在操作中使用的日期和时间",
                "nullable": false
            },
            "Modified_By": {
                "type": "Object",
                "comment": "修改商机的用户名称和ID。这是一个系统生成的字段。你不能修改它。",
                "nullable": false
            },
            "Lead_Conversion_Time": {
                "type": "Number",
                "comment": "将线索转换为商机所需的天数",
                "nullable": false
            },
            "_process_flow": {
                "type": "Boolean",
                "comment": "表示记录是否为蓝图数据",
                "nullable": false
            },
            "Deal_Name": {
                "type": "String",
                "comment": "商机名称。这是一个必填字段",
                "nullable": false
            },
            "Expected_Revenue": {
                "type": "Number",
                "comment": "根据您指定的金额和交易阶段，期望从该交易中获得的收入",
                "nullable": false
            },
            "Overall_Sales_Duration": {
                "type": "Number",
                "comment": "在不同类型的商机中，线索的平均天数",
                "nullable": false
            },
            "Stage": {
                "type": "String",
                "comment": "商机从合格到成交的当前阶段。这是一个必填字段",
                "nullable": false
            },
            "Account_Name": {
                "type": "Object",
                "comment": "商机关联的客户名称和ID",
                "nullable": false
            },
            "id": {
                "type": "String",
                "comment": "商机的唯一ID",
                "nullable": false,
                'isPrimaryKey':true,
                'primaryKeyPos':1
            },
            "_approved": {
                "type": "Boolean",
                "comment": "表示记录是否被批准",
                "nullable": false
            },
            "_approval": {
                "type": "Object",
                "comment": "表示当前用户是否可以批准、委托、拒绝或重新提交在此记录上执行的操作",
                "nullable": false
            },
            "Modified_Time": {
                "type": "String",
                "comment": "记录最后一次修改的日期和时间。这是一个系统生成的字段。你不能修改它。",
                "nullable": false
            },
            "Created_Time": {
                "type": "String",
                "comment": "记录创建的日期和时间。这是一个系统生成的字段。你不能修改它。",
                "nullable": false
            },
            "Amount": {
                "type": "Number",
                "comment": "商机完成后的预期金额",
                "nullable": false
            },
            "_followed": {
                "type": "Boolean",
                "comment": "表示当前用户是否遵循此协议",
                "nullable": false
            },
            "Probability": {
                "type": "Number",
                "comment": "表示完成此商机的概率",
                "nullable": false
            },
            "Next_Step": {
                "type": "String",
                "comment": "表示销售流程的下一步",
                "nullable": true
            },
            "_editable": {
                "type": "Boolean",
                "comment": "表示记录是否可编辑",
                "nullable": false
            },
            "Prediction_Score": {
                "type": "String",
                "comment": "表示商机的预测得分",
                "nullable": true
            },
            "Contact_Name": {
                "type": "Object",
                "comment": "与商机相关的联系人名称和ID",
                "nullable": false
            },
            "Sales_Cycle_Duration": {
                "type": "Number",
                "comment": "不同类型的商机成功关闭所需的平均天数",
                "nullable": false
            },
            "Type": {
                "type": "String",
                "comment": "表示商机类型",
                "nullable": true
            },
            "Lead_Source": {
                "type": "String",
                "comment": "表示线索来源",
                "nullable": false
            },
            "Created_By": {
                "type": "Object",
                "comment": "创建记录的用户名称和ID。这是一个系统生成的字段。你不能修改它",
                "nullable": false
            },
            "Tag": {
                "type": "Array",
                "comment": "与记录关联的标记列表",
                "nullable": false
            }
        }
    }, {
        "name":"Quotes",
        "fields":{
            "Owner": {
                "type": "Object",
                "comment": "报价单所有者的名称和ID",
                "nullable": false
            },
            "Discount": {
                "type": "Number",
                "comment": "表示价格上的折扣(如果有的话)",
                "nullable": false
            },
            "Description": {
                "type": "String",
                "comment": "报价单说明",
                "nullable": true
            },
            "_currency_symbol": {
                "type": "String",
                "comment": "产生收入的货币",
                "nullable": false
            },
            "Shipping_State": {
                "type": "String",
                "comment": "表示运输细节",
                "nullable": false
            },
            "Tax": {
                "type": "Number",
                "comment": "表示销售税和增值税之和",
                "nullable": false
            },
            "Modified_By": {
                "type": "Object",
                "comment": "修改报价单的用户名称和ID。这是一个系统生成的字段。你不能修改它",
                "nullable": false
            },
            "_converted": {
                "type": "Boolean",
                "comment": "表示报价单是否转换为销售订单或发货单",
                "nullable": false
            },
            "_process_flow": {
                "type": "Boolean",
                "comment": "表示记录是否为蓝图数据",
                "nullable": false
            },
            "Deal_Name": {
                "type": "Object",
                "comment": "创建报价单的商机名称和ID。这是一个必填字段。",
                "nullable": false
            },
            "Valid_Till": {
                "type": "String",
                "comment": "报价单有效之前的日期",
                "nullable": false
            },
            "Billing_Country": {
                "type": "String",
                "comment": "",
                "nullable": false
            },
            "Team": {
                "type": "String",
                "comment": "为其创建报价的团队名称",
                "nullable": false
            },
            "Account_Name": {
                "type": "Object",
                "comment": "创建报价的客户名称和ID。这是一个必填字段。",
                "nullable": false
            },
            "id": {
                "type": "String",
                "comment": "记录的唯一ID",
                "nullable": false,
                'isPrimaryKey':true,
                'primaryKeyPos':1
            },
            "Carrier": {
                "type": "String",
                "comment": "承运人名称",
                "nullable": false
            },
            "_approved": {
                "type": "Boolean",
                "comment": "表示记录是否被批准",
                "nullable": false
            },
            "Quote_Stage": {
                "type": "String",
                "comment": "表示报价的阶段",
                "nullable": false
            },
            "Grand_Total": {
                "type": "Number",
                "comment": "表示产品扣除税金和折扣后的总金额",
                "nullable": false
            },
            "_approval": {
                "type": "Object",
                "comment": "表示当前用户是否可以批准、委托、拒绝或重新提交在此记录上执行的操作",
                "nullable": false
            },
            "Modified_Time": {
                "type": "String",
                "comment": "记录最后一次修改的日期和时间。这是一个系统生成的字段。你不能修改它",
                "nullable": false
            },
            "Billing_Street": {
                "type": "String",
                "comment": "",
                "nullable": false
            },
            "Adjustment": {
                "type": "Number",
                "comment": "表示总金额中的调整(如果有的话)",
                "nullable": false
            },
            "Created_Time": {
                "type": "String",
                "comment": "记录创建的日期和时间。这是一个系统生成的字段。你不能修改它",
                "nullable": false
            },
            "Terms_and_Conditions": {
                "type": "String",
                "comment": "表示与报价关联的条款和条件",
                "nullable": true
            },
            "Sub_Total": {
                "type": "Number",
                "comment": "表示产品不含税的总额",
                "nullable": false
            },
            "_editable": {
                "type": "Boolean",
                "comment": "表示记录是否可编辑",
                "nullable": false
            },
            "Billing_Code": {
                "type": "String",
                "comment": "",
                "nullable": false
            },
            "Product_Details": {
                "type": "Array",
                "comment": "创建报价的产品明细",
                "nullable": false
            },
            "Subject": {
                "type": "String",
                "comment": "报价的主题/标题",
                "nullable": false
            },
            "Contact_Name": {
                "type": "Object",
                "comment": "创建报价的联系人名称和ID",
                "nullable": false
            },
            "Shipping_City": {
                "type": "String",
                "comment": "联系人的发货明细",
                "nullable": false
            },
            "Shipping_Country": {
                "type": "String",
                "comment": "",
                "nullable": false
            },
            "Shipping_Code": {
                "type": "String",
                "comment": "",
                "nullable": false
            },
            "Billing_City": {
                "type": "String",
                "comment": "",
                "nullable": false
            },
            "Quote_Number": {
                "type": "String",
                "comment": "",
                "nullable": false
            },
            "Billing_State": {
                "type": "String",
                "comment": "",
                "nullable": false
            },
            "_line_tax": {
                "type": "Array",
                "comment": "产品的销售税和增值税的百分比",
                "nullable": false
            },
            "Created_By": {
                "type": "Object",
                "comment": "创建记录的用户名称和ID。这是一个系统生成的字段。你不能修改它",
                "nullable": false
            },
            "Tag": {
                "type": "Array",
                "comment": "与记录关联的标记列表",
                "nullable": false
            },
            "Shipping_Street": {
                "type": "String",
                "comment": "",
                "nullable": false
            }
        }
    }, {
        "name":"Sales_Orders",
        "fields":{
            "Owner": {
                "type": "Object",
                "comment": "销售订单所有者的名称和ID",
                "nullable": false
            },
            "Discount": {
                "type": "Number",
                "comment": "表示价格上的折扣(如果有的话)",
                "nullable": false
            },
            "Description": {
                "type": "String",
                "comment": "销售订单说明",
                "nullable": true
            },
            "_currency_symbol": {
                "type": "String",
                "comment": "产生收入的货币",
                "nullable": false
            },
            "Customer_No": {
                "type": "String",
                "comment": "表示客户ID(如果有的话)",
                "nullable": true
            },
            "Shipping_State": {
                "type": "String",
                "comment": "联系人的收货地址",
                "nullable": false
            },
            "Tax": {
                "type": "Number",
                "comment": "销售税和增值税的总和",
                "nullable": false
            },
            "Modified_By": {
                "type": "Object",
                "comment": "修改记录的用户名称和ID。这是一个系统生成的字段。你不能修改它",
                "nullable": false
            },
            "_converted": {
                "type": "Boolean",
                "comment": "表示销售订单是否转换为发票",
                "nullable": false
            },
            "_process_flow": {
                "type": "Boolean",
                "comment": "表示记录是否为蓝图数据",
                "nullable": false
            },
            "Deal_Name": {
                "type": "Object",
                "comment": "需要为其创建销售订单的商机名称和ID",
                "nullable": false
            },
            "Billing_Country": {
                "type": "String",
                "comment": "",
                "nullable": false
            },
            "Account_Name": {
                "type": "Object",
                "comment": "必须为其创建销售订单的客户名和ID。这个字段是必填的",
                "nullable": false
            },
            "id": {
                "type": "String",
                "comment": "记录的唯一ID",
                "nullable": false,
                'isPrimaryKey':true,
                'primaryKeyPos':1
            },
            "Carrier": {
                "type": "String",
                "comment": "承运人名称",
                "nullable": false
            },
            "_approved": {
                "type": "Boolean",
                "comment": "表示记录是否被批准",
                "nullable": false
            },
            "Quote_Name": {
                "type": "Object",
                "comment": "参考报价单的名称和ID",
                "nullable": false
            },
            "Status": {
                "type": "String",
                "comment": "表示销售订单的状态",
                "nullable": false
            },
            "Sales_Commission": {
                "type": "Number",
                "comment": "在完成交易后，代表销售人员的佣金",
                "nullable": false
            },
            "Grand_Total": {
                "type": "Number",
                "comment": "表示产品扣除税金和折扣后的总金额",
                "nullable": false
            },
            "_approval": {
                "type": "Object",
                "comment": "表示当前用户是否可以批准、委托、拒绝或重新提交在此记录上执行的操作",
                "nullable": false
            },
            "Modified_Time": {
                "type": "String",
                "comment": "记录最后一次修改的日期和时间。这是一个系统生成的字段。你不能修改它",
                "nullable": false
            },
            "Due_Date": {
                "type": "String",
                "comment": "销售订单到期的日期",
                "nullable": false
            },
            "Billing_Street": {
                "type": "String",
                "comment": "",
                "nullable": false
            },
            "Adjustment": {
                "type": "Number",
                "comment": "表示总金额中的调整(如果有的话)",
                "nullable": false
            },
            "Created_Time": {
                "type": "String",
                "comment": "记录创建的日期和时间。这是一个系统生成的字段。你不能修改它",
                "nullable": false
            },
            "Terms_and_Conditions": {
                "type": "String",
                "comment": "表示与采购订单关联的条款和条件",
                "nullable": true
            },
            "Sub_Total": {
                "type": "Number",
                "comment": "表示产品不含税的总额",
                "nullable": false
            },
            "_editable": {
                "type": "Boolean",
                "comment": "表示记录是否可编辑",
                "nullable": false
            },
            "Billing_Code": {
                "type": "String",
                "comment": "",
                "nullable": true
            },
            "Product_Details": {
                "type": "Array",
                "comment": "为其创建销售订单的产品详细信息",
                "nullable": false
            },
            "Subject": {
                "type": "String",
                "comment": "销售订单的主题/标题",
                "nullable": false
            },
            "Contact_Name": {
                "type": "Object",
                "comment": "为其创建销售订单的联系人名称和ID",
                "nullable": false
            },
            "Excise_Duty": {
                "type": "Number",
                "comment": "产品的消费税",
                "nullable": false
            },
            "Shipping_City": {
                "type": "String",
                "comment": "",
                "nullable": false
            },
            "Shipping_Country": {
                "type": "String",
                "comment": "",
                "nullable": false
            },
            "Shipping_Code": {
                "type": "String",
                "comment": "",
                "nullable": true
            },
            "Billing_City": {
                "type": "String",
                "comment": "",
                "nullable": false
            },
            "SO_Number": {
                "type": "String",
                "comment": "创建服务支持后的销售订单ID",
                "nullable": false
            },
            "Purchase_Order": {
                "type": "String",
                "comment": "参考采购订单",
                "nullable": true
            },
            "Billing_State": {
                "type": "String",
                "comment": "",
                "nullable": false
            },
            "_line_tax": {
                "type": "Array",
                "comment": "产品的销售税和增值税的百分比",
                "nullable": false
            },
            "Created_By": {
                "type": "Object",
                "comment": "创建记录的用户名称和ID。这是一个系统生成的字段。你不能修改它",
                "nullable": false
            },
            "Tag": {
                "type": "Array",
                "comment": "与记录关联的标记列表",
                "nullable": false
            },
            "Pending": {
                "type": "String",
                "comment": "",
                "nullable": true
            },
            "Shipping_Street": {
                "type": "String",
                "comment": "",
                "nullable": false
            }
        }
    }
    ]
}

/**
 *
 * @param connectionConfig  Configuration property information of the connection page
 * @param nodeConfig  Configuration attribute information of node page
 * @param offset Breakpoint information, which can save paging condition information
 * @param tableName  The name of the table to obtain the full amount of phase data
 * @param pageSize Processing number of each batch of data in the full stage
 * @param batchReadSender  Sender of submitted data
 * */
function batchRead(connectionConfig, nodeConfig, offset, tableName, pageSize, batchReadSender) {
    if(!offset || !offset.tableName){
        offset = {
            page:1,
            tableName:tableName
        };
    }
    iterateAllData('getData', offset, (result, offsetNext) => {
        let haveNext = false;
        if(result && result !== ''){
            if(result.data){
                for(let x in result.data){
                    result.data[x] = handleData(result.data[x]);
                }
                batchReadSender.send(result.data,tableName,offset);
            }
            if(result.info && result.info.more_records && result.info.page){
                if(!offsetNext.page){
                    //offsetNext.page = 1;
                }
                offsetNext.page = offsetNext.page + 1;
                haveNext = true;
            }
            if(!haveNext){
                return false
            }
        }
        return isAlive() && haveNext;
    });
    batchReadSender.send(offset);
}


/**
 *
 * @param connectionConfig  Configuration property information of the connection page
 * @param nodeConfig
 * @param offset
 * @param tableNameList
 * @param pageSize
 * @param streamReadSender
 * */
var batchStart = dateUtils.nowDate();
var startTime = new Date();
function streamRead(connectionConfig, nodeConfig, offset, tableNameList, pageSize, streamReadSender) {
    if (!isParam(offset) || null == offset || typeof(offset) != 'object') offset = {};
    let now = new Date();
    for(let x in tableNameList) {
      let tableName = tableNameList[x];
      let isFirst = false;
      if(!offset[tableName]){
        offset[tableName] = {tableName:tableName, page: 1, Conditions:[{Key: 'UPDATED_AT',Value: batchStart + '_' + dateUtils.nowDate()}]} ;
        isFirst = true;
      }
        let condition = arrayUtils.firstElement(offset[tableName].Conditions);
        offset[tableName].Conditions = [{Key:"UPDATED_AT",Value: isParam(condition) && null != condition ? arrayUtils.firstElement(condition.Value.split('_')) + '_' + dateUtils.nowDate(): batchStart + '_' + dateUtils.nowDate()}];
        if(!offset[tableName]['If-Modified-Since']) {
            offset[tableName]['If-Modified-Since'] = dateUtils.timeStamp2Date((startTime.getTime() - 5000)+"", "yyyy-MM-dd'T'HH:mm:ssXXX");//new Date(startTime.getTime() - 5000).toISOString();
        }
        iterateAllData('getDataA', offset[tableName], (result, offsetNext, error) => {
            let haveNext = false;
            let arr = [];
            if(result && result !== ''){
                if(result.info && result.info.more_records && result.info.page){
                    if(!offsetNext.page){
                        offsetNext.page = 1;
                    }
                    offsetNext.page = offsetNext.page + 1;
                    haveNext = true;
                }
                for(let x in result.data){
                    let item = {
                        "eventType": "i",
                        "tableName": tableName,
                        "afterData": handleData(result.data[x]),
                        "referenceTime":  Number(new Date())
                    };
                    if (result.data[x].Created_Time === result.data[x].Modified_Time) {
                        item.eventType = 'i';
                    } else {
                        item.eventType = 'u';
                    }
                    arr.push(item);
                }
                streamReadSender.send(arr,tableName,offset);
                if(!haveNext){
                    offset[tableName].page = 1;
                    offset[tableName]['If-Modified-Since'] = dateUtils.timeStamp2Date((now.getTime())+"", "yyyy-MM-dd'T'HH:mm:ssXXX");//new Date(now.getTime() - 5000).toISOString();
                    return false
                }
            }
            return isAlive() && haveNext;
        });
    }
    streamReadSender.send(offset);
}


/**
 * @return The returned result is not empty and must be in the following form:
 *          [
 *              {"test": String, "code": Number, "result": String},
 *              {"test": String, "code": Number, "result": String},
 *              ...
 *          ]
 *          param - test :  The type is a String, representing the description text of the test item.
 *          param - code :  The type is a Number, is the type of test result. It can only be [-1, 0, 1], -1 means failure, 1 means success, 0 means warning.
 *          param - result : The type is a String, descriptive text indicating test results.
 * @param connectionConfig  Configuration property information of the connection page
 * */
function connectionTest(connectionConfig) {
    return [{
        "test": "Example test item",
        "code": 1,
        "result": "Pass"
    },{
        "test": "Read log",
        "code": 1,
        "result": "Pass"
    }];
}


/**
 *
 * @param connectionConfig
 * @param nodeConfig
 * @param commandInfo
 * */
let clientInfo = {
    "client_id": "1000.HUROTBTBLYFWDUE4JHFDCS2Q28EP9V",
    "client_secret": "c3785a4e07b57e1c86e8078ddca424bf30d646b09e"
}
function commandCallback(connectionConfig, nodeConfig, commandInfo) {
    if (commandInfo.command === 'OAuth'){
        let obj = {code:connectionConfig.code};
        Object.assign(obj,clientInfo)
        let getToken = invoker.invokeWithoutIntercept("getToken",obj);
        if(getToken.result){
            connectionConfig.access_token = getToken.result.access_token;
            connectionConfig.refreshToken = getToken.result.refresh_token
        }
        return connectionConfig;
    }
    if (commandInfo.command === 'TokenCommand') {
        let body = {
            client_id:connectionConfig.client_id,
            client_secret:connectionConfig.client_secret,
            code:connectionConfig.code
        };
        let refreshToken = invoker.invoke("getToken",body,"POST");
        if (refreshToken && refreshToken.result && refreshToken.result.access_token) {
            return {
                'setValue':{
                    accessToken:{data:refreshToken.result.access_token},
                    refreshToken:{data:refreshToken.result.refresh_token},
                    getTokenMsg:{data:'OK'}
                }
            };
        }else{
           return {'setValue':{
               getTokenMsg:{data:refreshToken.result.error}
             }}
        }
    }
}


/**
 * This method is used to update the access key
 *  @param connectionConfig :type is Object
 *  @param nodeConfig :type is Object
 *  @param apiResponse :The following valid data can be obtained from apiResponse : {
 *              result :  type is Object, Return result of interface call
 *              httpCode : type is Integer, Return http code of interface call
 *          }
 *
 *  @return must be {} or null or {"key":"value",...}
 *      - {} :  Type is Object, but not any key-value, indicates that the access key does not need to be updated, and each interface call directly uses the call result
 *      - null : Semantics are the same as {}
 *      - {"key":"value",...} : Type is Object and has key-value ,  At this point, these values will be used to call the interface again after the results are returned.
 * */
function updateToken(connectionConfig, nodeConfig, apiResponse) {
    if (apiResponse.httpCode === 401 || (apiResponse.result && apiResponse.result.code === 'INVALID_TOKEN' || apiResponse.result.code === 'AUTHENTICATION_FAILURE')) {
        try{
            let refreshToken = invoker.invokeWithoutIntercept("refreshToken");
            if(refreshToken && refreshToken.result &&refreshToken.result.access_token){
                return {"accessToken": refreshToken.result.access_token};
            }
        }catch (e) {
            log.warn(e)
        }
    }
    return null;
}

function handleData(data) {
    for(let x in data){
        if(data[x] !== undefined && data[x] !== null) {
            let sss = data[x].toString()
            if (Array.isArray(data[x])) {
                data[x] = handleData(data[x])
            } else if (sss === "[object Object]") {
                data[x] = handleData(data[x])
            }
        }
        if(x && x.startsWith('$')){
            let key = x.replace('$','_');
            data[key] = data[x];
            delete data[x];
        }
    }
    return data
}