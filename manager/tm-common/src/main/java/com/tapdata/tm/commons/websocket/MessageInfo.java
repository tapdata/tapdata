package com.tapdata.tm.commons.websocket;

import lombok.Data;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/4/13 下午5:07
 */
@Data
public abstract class MessageInfo {

    private String version;
    private String reqId;

    public abstract String toTextMessage();

}
