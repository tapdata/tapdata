package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.TapEvent;

public class TapAlterTableCharsetEvent extends TapTableEvent {
    public static final int TYPE = 204;
    private String charset;
    public TapAlterTableCharsetEvent charset(String charset) {
        this.charset = charset;
        return this;
    }

    public TapAlterTableCharsetEvent() {
        super(TYPE);
    }
    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if (tapEvent instanceof TapAlterTableCharsetEvent) {
            TapAlterTableCharsetEvent alterFieldDataTypeEvent = (TapAlterTableCharsetEvent) tapEvent;
            if (charset != null)
                alterFieldDataTypeEvent.charset = charset;
        }
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }
}
