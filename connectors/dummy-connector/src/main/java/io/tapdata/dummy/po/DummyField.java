package io.tapdata.dummy.po;

/**
 * Dummy field vo
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/7/6 10:55 Create
 */
public class DummyField {
    private String type;
    private String name;
    private String len;
    private String def;
    private Boolean pri;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLen() {
        return len;
    }

    public void setLen(String len) {
        this.len = len;
    }

    public String getDef() {
        return def;
    }

    public void setDef(String def) {
        this.def = def;
    }

    public Boolean getPri() {
        return pri;
    }

    public void setPri(Boolean pri) {
        this.pri = pri;
    }

    public Object getDefValue() {
        return null;
    }
}
