package com.tapdata.tm.commons.websocket.v1;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/4/16 上午11:31
 */
@Getter
@Setter
@ToString
@EqualsAndHashCode
public class ResultWrap {

    public static final String OK = "ok";

    /**
     * 请求处理的代码
     */
    protected String code = OK;

    /**
     * 请求处理失败时的错误消息
     */
    protected String message;

    /**
     * 请求处理成功的数据
     */
    protected String data;
}
