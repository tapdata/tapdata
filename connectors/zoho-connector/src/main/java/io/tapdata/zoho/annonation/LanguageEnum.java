package io.tapdata.zoho.annonation;

public enum LanguageEnum {
    ZH_CN("zh_CN"),
    ZH_TW("zh_TW"),
    EN("en_US");
    String language;
    LanguageEnum(String language){
        this.language = language;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }
}
