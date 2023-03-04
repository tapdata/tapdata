package io.tapdata.zoho.enums;

import io.tapdata.zoho.utils.Checker;
//Desk.tickets.ALL,Desk.contacts.READ,Desk.contacts.WRITE,Desk.contacts.UPDATE,Desk.contacts.CREATE,Desk.tasks.ALL,Desk.basic.READ,Desk.basic.CREATE,Desk.settings.ALL,Desk.search.READ,Desk.events.ALL,Desk.articles.READ,Desk.articles.CREATE,Desk.articles.UPDATE,Desk.articles.DELETE
public enum CustomFieldType {
    //单行文本 String(1-255)
    //多行文本 String(5000)
    //整数     Integer(1-9)
    //百分数   Float(1-5,0-2)
    //小数     Double(1-16,0-9)
    //货币     Double(1-16,0-9)
    //日期     yyyy-MM-dd   String(16)
    //日期时间 yyyy-MM-dd hh:mm:ss String(32)
    //邮箱     String(254)
    //电话     String(1-255)
    //单选     String(32)
    //多选     String(1024)
    //URL     String(2083)
    //检查框   Boolean
    DEFAULT("","StringNormal","默认字符串处理","", String.class ),
    /**
     * {
     *     "displayLabel": "单行 1",
     *     "apiName": "cf_dan_xing_1",
     *     "isCustomField": true,
     *     "showToHelpCenter": true,
     *     "i18NLabel": "单行 1",
     *     "name": "单行 1",
     *     "isEncryptedField": false,
     *     "id": "10504000000178050",
     *     "type": "Text",
     *     "maxLength": 64,
     *     "isMandatory": false
     * }
     * */
    Text("Text","Text","字符串字段（单行、选择列表、邮箱、电话和URL）","",String.class),
    /**
     * {
     *     "displayLabel": "多行 2",
     *     "apiName": "cf_duo_xing_2",
     *     "isCustomField": true,
     *     "showToHelpCenter": true,
     *     "i18NLabel": "多行 2",
     *     "name": "多行 2",
     *     "isEncryptedField": false,
     *     "id": "10504000000178124",
     *     "type": "Textarea",
     *     "maxLength": 5000,
     *     "isMandatory": false
     * }
     * */
    TEXTAREA("Textarea","Textarea","","",String.class),
    /**
     * {
     *     "displayLabel": "整数 2",
     *     "apiName": "cf_zheng_shu_2",
     *     "isCustomField": true,
     *     "showToHelpCenter": true,
     *     "i18NLabel": "整数 2",
     *     "name": "整数 2",
     *     "isEncryptedField": false,
     *     "id": "10504000000178200",
     *     "type": "Number",
     *     "maxLength": 1,
     *     "isMandatory": false
     * }
     * */
    NUMBER("Number","Number","","",Integer.class),
    /**
     * {
     *     "displayLabel": "百分数 1",
     *     "apiName": "cf_bai_fen_shu_1",
     *     "isCustomField": true,
     *     "showToHelpCenter": true,
     *     "i18NLabel": "百分数 1",
     *     "name": "百分数 1",
     *     "isEncryptedField": false,
     *     "id": "10504000000178278",
     *     "type": "Percent",
     *     "maxLength": 5,
     *     "isMandatory": false
     * }
     * */
    PERCENT("Percent","String","","",String.class),
    /**
     * {
     *     "displayLabel": "小数 1",
     *     "apiName": "cf_xiao_shu_1",
     *     "isCustomField": true,
     *     "decimalPlaces": 9,
     *     "showToHelpCenter": true,
     *     "i18NLabel": "小数 1",
     *     "name": "小数 1",
     *     "isEncryptedField": false,
     *     "id": "10504000000178358",
     *     "type": "Decimal",
     *     "maxLength": 16,
     *     "isMandatory": false
     * }
     * */
    DECIMAL("Decimal","String","","",String.class),
    /**
     * {
     *     "displayLabel": "货币 1",
     *     "apiName": "cf_huo_bi_1",
     *     "isCustomField": true,
     *     "showToHelpCenter": true,
     *     "i18NLabel": "货币 1",
     *     "roundingPrecision": 5,
     *     "type": "Currency",
     *     "decimalPlaces": 2,
     *     "roundingOption": "normal",
     *     "name": "货币 1",
     *     "isEncryptedField": false,
     *     "id": "10504000000178440",
     *     "maxLength": 16,
     *     "isMandatory": false
     * }
     * */
    CURRENCY("Currency","String","","",String.class),
    /**
     * {
     *     "displayLabel": "日期 1",
     *     "apiName": "cf_ri_qi_1",
     *     "isCustomField": true,
     *     "showToHelpCenter": true,
     *     "i18NLabel": "日期 1",
     *     "name": "日期 1",
     *     "isEncryptedField": false,
     *     "id": "10504000000178524",
     *     "type": "Date",
     *     "maxLength": 20,
     *     "isMandatory": false
     * }
     * */
    DATE("Date","String","","",String.class),
    /**
    * {
    *     "displayLabel": "日期时间 1",
    *     "apiName": "cf_ri_qi_shi_jian_1",
    *     "isCustomField": true,
    *     "showToHelpCenter": true,
    *     "i18NLabel": "日期时间 1",
    *     "name": "日期时间 1",
    *     "isEncryptedField": false,
    *     "id": "10504000000178610",
    *     "type": "DateTime",
    *     "maxLength": 120,
    *     "isMandatory": false
    * }
    */
    DATETIME("DateTime","String","","",String.class),
    /**
     * {
     *     "displayLabel": "电子邮件 1",
     *     "apiName": "cf_dian_zi_you_jian_1",
     *     "isCustomField": true,
     *     "showToHelpCenter": true,
     *     "i18NLabel": "电子邮件 1",
     *     "name": "电子邮件 1",
     *     "isEncryptedField": false,
     *     "id": "10504000000178698",
     *     "type": "Email",
     *     "maxLength": 254,
     *     "isMandatory": false
     * }
     * */
    EMAIL("Email","String","","",String.class),
    /**
     * {
     *     "displayLabel": "电话 1",
     *     "apiName": "cf_dian_hua_1",
     *     "isCustomField": true,
     *     "showToHelpCenter": true,
     *     "i18NLabel": "电话 1",
     *     "name": "电话 1",
     *     "isEncryptedField": false,
     *     "id": "10504000000178788",
     *     "type": "Phone",
     *     "maxLength": 255,
     *     "isMandatory": false
     * }
     * */
    PHONE("Phone","String","","",String.class),
    /**
     * {
     *     "displayLabel": "单选框 1",
     *     "allowedValues": [
     *         {
     *             "value": "选项1"
     *         },
     *         {
     *             "value": "选项2"
     *         }
     *     ],
     *     "apiName": "cf_dan_xuan_kuang_1",
     *     "isCustomField": true,
     *     "showToHelpCenter": true,
     *     "i18NLabel": "单选框 1",
     *     "name": "单选框 1",
     *     "isEncryptedField": false,
     *     "id": "10504000000178880",
     *     "type": "Picklist",
     *     "maxLength": 120,
     *     "isMandatory": false
     * }
     * */
    PICK_LIST("Picklist","String","","",String.class),
    /**
     * {
     *     "displayLabel": "多选框 1",
     *     "allowedValues": [
     *         {
     *             "value": "选项1"
     *         },
     *         {
     *             "value": "选项2"
     *         }
     *     ],
     *     "apiName": "cf_duo_xuan_kuang_1",
     *     "isCustomField": true,
     *     "showToHelpCenter": true,
     *     "i18NLabel": "多选框 1",
     *     "name": "多选框 1",
     *     "isEncryptedField": false,
     *     "id": "10504000000178974",
     *     "type": "Multiselect",
     *     "maxLength": 120,
     *     "isMandatory": false
     * }
     * */
    MULTI_SELECT("Multiselect","String","","",String.class),
    /**
     * {
     *     "displayLabel": "URL 1",
     *     "apiName": "cf_url_1",
     *     "isCustomField": true,
     *     "showToHelpCenter": true,
     *     "i18NLabel": "URL 1",
     *     "name": "URL 1",
     *     "isEncryptedField": false,
     *     "id": "10504000000179070",
     *     "type": "URL",
     *     "maxLength": 2083,
     *     "isMandatory": false
     * }
     * */
    URL("URL","String","","{\"key\":2083}",String.class),
    /**
     * {
     *     "displayLabel": "检查框 1",
     *     "apiName": "cf_jian_cha_kuang_1",
     *     "isCustomField": true,
     *     "defaultValue": "false",
     *     "showToHelpCenter": true,
     *     "i18NLabel": "检查框 1",
     *     "name": "检查框 1",
     *     "isEncryptedField": false,
     *     "id": "10504000000179168",
     *     "type": "Boolean",
     *     "maxLength": 3,
     *     "isMandatory": false
     * }
     * */
    BOOLEAN("Boolean","Boolean","","",Boolean.class)
    ;
    String type;
    String name;
    String desc;
    String attr;
    private Class feature;
    CustomFieldType(String name,String type,String desc,String attr,Class feature){
        this.name = name;
        this.feature = feature;
        this.type = type;
        this.desc = desc;
        this.attr = attr;
    }
    public static String type(String name){
        if (Checker.isEmpty(name)) return DEFAULT.getType();
        CustomFieldType[] values = CustomFieldType.values();
        for (CustomFieldType value : values) {
            if (name.equals(value.getName())){
                return value.getType();
            }
        }
        return DEFAULT.getType();
    }
    public static Class feature(String name){
        if (Checker.isEmpty(name)) return DEFAULT.getFeature();
        CustomFieldType[] values = CustomFieldType.values();
        for (CustomFieldType value : values) {
            if (name.equals(value.getName())){
                return value.getFeature();
            }
        }
        return DEFAULT.getFeature();
    }


    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDec() {
        return desc;
    }

    public void setDec(String dec) {
        this.desc = dec;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public Class getFeature() {
        return feature;
    }

    public void setFeature(Class feature) {
        this.feature = feature;
    }

    public String getAttr() {
        return attr;
    }

    public void setAttr(String attr) {
        this.attr = attr;
    }
}
