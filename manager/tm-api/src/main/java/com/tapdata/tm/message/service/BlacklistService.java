package com.tapdata.tm.message.service;

public interface BlacklistService {
    boolean inBlacklist(String email);

    boolean inBlacklist(String countryCode, String phone);
}
