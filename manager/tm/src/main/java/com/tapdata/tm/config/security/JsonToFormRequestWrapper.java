package com.tapdata.tm.config.security;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Enumeration;

public class JsonToFormRequestWrapper extends HttpServletRequestWrapper {
    private final Map<String, String> originBody;

    public JsonToFormRequestWrapper(HttpServletRequest request,
                                    Map<String, String> jsonBody) {
        super(request);
        this.originBody = new HashMap<>(jsonBody);
    }

    @Override
    public String getParameter(String name) {
        String value = this.originBody.get(name);
        return value != null ? value : super.getParameter(name);
    }

    @Override
    public Map<String, String[]> getParameterMap() {
        Map<String, String[]> all = new HashMap<>(super.getParameterMap());
        this.originBody.forEach((key, value) -> all.put(key, new String[]{value}));
        return all;
    }

    public Map<String, String> originBody() {
        return originBody;
    }

    @Override
    public Enumeration<String> getParameterNames() {
        return Collections.enumeration(getParameterMap().keySet());
    }

    @Override
    public String[] getParameterValues(String name) {
        return getParameterMap().get(name);
    }
}
