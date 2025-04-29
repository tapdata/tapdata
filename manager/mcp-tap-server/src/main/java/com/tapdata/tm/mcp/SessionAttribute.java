package com.tapdata.tm.mcp;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2025/3/26 11:39
 */
public interface SessionAttribute {

    Object getAttribute(String sessionId, String key);

    /**
     * Put attribute into session
     * @param sessionId session id
     * @param key key
     * @param value value
     * @return return exists value for key or else null.
     */
    Object setAttribute(String sessionId, String key, Object value);
}
