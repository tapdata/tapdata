package io.tapdata.connector.tdengine.bean;

import java.io.Serializable;

/**
 * offset for batch read
 *
 * @author Jarad
 * @date 2022/5/09
 */
public class TDengineOffset implements Serializable {

    private Long offsetValue;

    private String sourceOffset;

    public TDengineOffset() {
    }

    public TDengineOffset(Long offsetValue) {
        this.offsetValue = offsetValue;
    }

    public Long getOffsetValue() {
        return offsetValue;
    }

    public void setOffsetValue(Long offsetValue) {
        this.offsetValue = offsetValue;
    }

    public String getSourceOffset() {
        return sourceOffset;
    }

    public void setSourceOffset(String sourceOffset) {
        this.sourceOffset = sourceOffset;
    }
}
