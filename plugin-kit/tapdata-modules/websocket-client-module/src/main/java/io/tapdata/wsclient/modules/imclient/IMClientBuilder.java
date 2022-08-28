package io.tapdata.wsclient.modules.imclient;

import io.tapdata.wsclient.modules.imclient.impls.IMClientImpl;

public class IMClientBuilder {
    private String prefix;
    private String userId;
    private String service;
    private Integer terminal;
    private String token;
    private String loginUrl;

    public IMClientBuilder() {
    }

    public IMClientBuilder withPrefix(String prefix) {
        this.prefix = prefix;
        return this;
    }
    public IMClientBuilder withUserId(String userId) {
        this.userId = userId;
        return this;
    }
    public IMClientBuilder withService(String service) {
        this.service = service;
        return this;
    }
    public IMClientBuilder withTerminal(Integer terminal) {
        this.terminal = terminal;
        return this;
    }
    public IMClientBuilder withToken(String token) {
        this.token = token;
        return this;
    }
    public IMClientBuilder withLoginUrl(String loginUrl) {
        this.loginUrl = loginUrl;
        return this;
    }

    public IMClient build() {
        if(this.prefix == null ||
                this.userId == null ||
                this.service == null ||
                this.terminal == null ||
                this.token == null ||
                this.loginUrl == null)
            throw new IllegalArgumentException("Build IMClient failed, need userId, service, terminal, token and loginUrl");
        return new IMClientImpl(prefix, userId, service, terminal, token, loginUrl);
    }
}
