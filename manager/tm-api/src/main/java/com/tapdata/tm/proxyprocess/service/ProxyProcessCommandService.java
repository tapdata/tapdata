package com.tapdata.tm.proxyprocess.service;

import com.tapdata.tm.proxyprocess.dto.ProxyProcessCommandResult;

public interface ProxyProcessCommandService {
    ProxyProcessCommandResult execute(String proxyProcessId, String type, Object value);
}
