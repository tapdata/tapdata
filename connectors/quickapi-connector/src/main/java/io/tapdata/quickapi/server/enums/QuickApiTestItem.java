package io.tapdata.quickapi.server.enums;

public enum QuickApiTestItem {
    TEST_PARAM("Test connection params"),
    TEST_JSON_FORMAT("Check Api JSON"),
    TEST_TAP_TABLE("Check TAP_TABLE label format"),
    TEST_TOKEN("Check Token environment variable"),
    DEBUG_API("API connection debugging");
    String testName;
    QuickApiTestItem(String testName){
        this.testName = testName;
    }
    public String testName(){
        return this.testName;
    }
}
