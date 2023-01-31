package io.tapdata.coding.enums;

/**
 * @author Gavin
 * @Description CodingTestItem
 * @create 2022-08-24 10:16
 **/
public enum CodingTestItem {
    CONNECTION_TEST("Check your team name"),
    TOKEN_TEST("Check whether the Token is correct"),
    PROJECT_TEST("Check your project"),
    ;
    String content;

    CodingTestItem(String content) {
        this.content = content;
    }

    public String getContent() {
        return content;
    }
}
