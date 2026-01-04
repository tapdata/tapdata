package com.tapdata.tm.config.security;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Enumeration;

public class JsonToFormRequestWrapper extends HttpServletRequestWrapper {

    private final Map<String, String[]> params = new HashMap<>();

    public JsonToFormRequestWrapper(HttpServletRequest request,
                                    Map<String, String> jsonBody) {
        super(request);
        jsonBody.forEach((k, v) -> params.put(k, new String[]{v}));
    }

    @Override
    public String getParameter(String name) {
        String[] values = params.get(name);
        return values != null ? values[0] : super.getParameter(name);
    }

    @Override
    public Map<String, String[]> getParameterMap() {
        Map<String, String[]> all = new HashMap<>(super.getParameterMap());
        all.putAll(params);
        return all;
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
