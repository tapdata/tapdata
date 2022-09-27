package io.tapdata.zoho.service.zoho;

public interface ZoHoBase {
    String ZO_HO_BASE_URL = "https://desk.zoho.com.cn%s";
    String ZO_HO_BASE_TOKEN_URL="https://accounts.zoho.com.cn%s";
    public String ZO_HO_ACCESS_TOKEN_PREFIX = "Zoho-oauthtoken ";
    String ZO_HO_BASE_SCOPE = "Desk.tickets.ALL,Desk.contacts.READ,Desk.contacts.WRITE,Desk.contacts.UPDATE,Desk.contacts.CREATE,Desk.tasks.ALL,Desk.basic.READ,Desk.basic.CREATE,Desk.settings.ALL,Desk.events.ALL,Desk.articles.READ,Desk.articles.CREATE,Desk.articles.UPDATE,Desk.articles.DELETE";
}
