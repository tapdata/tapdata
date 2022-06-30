package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.entity.FieldAttrChange;

import java.util.ArrayList;
import java.util.List;

public class TapAlterTableCharsetEvent extends TapTableEvent {
    public static final int TYPE = 200;
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
