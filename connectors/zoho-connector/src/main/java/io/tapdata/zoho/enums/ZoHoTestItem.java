package io.tapdata.zoho.enums;

public enum ZoHoTestItem {
    TOKEN_TEST("Check whether the Token is correct");
    String content;

    ZoHoTestItem(String content) {
        this.content = content;
    }

    public String getContent() {
        return content;
    }
}
