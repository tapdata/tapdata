package io.tapdata.wsclient.modules.imclient;

import io.tapdata.wsclient.modules.imclient.impls.IMClientImpl;

import java.util.List;

public class IMClientBuilder {
    private String prefix;
    private String clientId;
    private String service;
    private Integer terminal;
    private String token;
    private List<String> baseUrls;

    public IMClientBuilder() {
    }

    public IMClientBuilder withPrefix(String prefix) {
        this.prefix = prefix;
        return this;
    }
    public IMClientBuilder withClientId(String clientId) {
        this.clientId = clientId;
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
    public IMClientBuilder withBaseUrl(List<String> baseUrls) {
        this.baseUrls = baseUrls;
        return this;
    }

    public IMClient build() {
        if(this.prefix == null ||
                this.clientId == null ||
                this.service == null ||
                this.terminal == null ||
                this.token == null ||
                this.baseUrls == null)
            throw new IllegalArgumentException("Build IMClient failed, need clientId, service, terminal, token and baseUrls");
        return new IMClientImpl(prefix, clientId, service, terminal, token, baseUrls);
    }
}
