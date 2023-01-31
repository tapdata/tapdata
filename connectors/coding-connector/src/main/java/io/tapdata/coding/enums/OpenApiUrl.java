package io.tapdata.coding.enums;

public enum OpenApiUrl {
    CONNECTION_URL("https://%{s}.coding.net"),
    OPEN_API_URL("https://%{s}.coding.net/open-api"),//%{s}---》teamName
    TOKEN_URL("https://%{s}.coding.net/api/me");
    String url;

    OpenApiUrl(String url) {
        this.url = url;
    }

    public String getUrl() {
        return this.url;
    }
}
