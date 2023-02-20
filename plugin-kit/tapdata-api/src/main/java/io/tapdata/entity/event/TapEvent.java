package io.tapdata.entity.event;

import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.serializer.JavaCustomSerializer;
import io.tapdata.entity.utils.FormatUtils;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class TapEvent implements Serializable/*, JavaCustomSerializer*/ {
    protected int type;
    /**
     * The time when the event is created
     */
    protected Long time;

    protected Map<String, Object> info;

    protected Map<String, Object> traceMap;
    protected String key;

    protected String pdkId;
    protected String pdkGroup;
    protected String pdkVersion;
    /*public void from(InputStream inputStream) throws IOException {
        DataInputStreamEx dataInputStreamEx = dataInputStream(inputStream);
        type = dataInputStreamEx.original().readInt();
        time = dataInputStreamEx.readLong();
        key = dataInputStreamEx.readUTF();
        pdkId = dataInputStreamEx.readUTF();
        pdkGroup = dataInputStreamEx.readUTF();
        pdkVersion = dataInputStreamEx.readUTF();
//        info and trace map will not be serialized
    }
    public void to(OutputStream outputStream) throws IOException {
        DataOutputStreamEx dataOutputStreamEx = dataOutputStream(outputStream);
        dataOutputStreamEx.original().writeInt(type);
        dataOutputStreamEx.writeLong(time);
        dataOutputStreamEx.writeUTF(key);
        dataOutputStreamEx.writeUTF(pdkId);
        dataOutputStreamEx.writeUTF(pdkGroup);
        dataOutputStreamEx.writeUTF(pdkVersion);
//        info and trace map will not be serialized
    }*/
    public TapEvent(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public Map<String, Object> getTraceMap() {
        return traceMap;
    }

    public void setTraceMap(Map<String, Object> traceMap) {
        this.traceMap = traceMap;
    }

    public Map<String, Object> getInfo() {
        return info;
    }

    public void setInfo(Map<String, Object> info) {
        this.info = info;
    }

    public Object addInfo(String key, Object value) {
        initInfo();
        return info.put(key, value);
    }

    private void initInfo() {
        if(info == null) {
            synchronized (this) {
                if(info == null) {
                    info = new LinkedHashMap<>();
                }
            }
        }
    }

    public Object removeInfo(String key) {
        initInfo();
        return info.remove(key);
    }

    public Object getInfo(String key) {
        initInfo();
        return info.get(key);
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public String getPdkId() {
        return pdkId;
    }

    public void setPdkId(String pdkId) {
        this.pdkId = pdkId;
    }

    public String getPdkGroup() {
        return pdkGroup;
    }

    public void setPdkGroup(String pdkGroup) {
        this.pdkGroup = pdkGroup;
    }

    public String getPdkVersion() {
        return pdkVersion;
    }

    public void setPdkVersion(String pdkVersion) {
        this.pdkVersion = pdkVersion;
    }

    @Override
    public Object clone() {
        try {
            TapEvent obj = this.getClass().newInstance();
            clone(obj);
            return obj;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public void clone(TapEvent tapEvent) {
        tapEvent.time = time;
        tapEvent.pdkId = pdkId;
        tapEvent.pdkGroup = pdkGroup;
        tapEvent.pdkVersion = pdkVersion;
        if(info != null)
            tapEvent.info = new ConcurrentHashMap<>(info);
        if(traceMap != null)
            tapEvent.traceMap = new ConcurrentHashMap<>(traceMap);
    }

    @Override
    public String toString() {
        TapEvent cloneEvent = (TapEvent) this.clone();
        // Filter info map when to json, will cause stackOverFlowError
        cloneEvent.setInfo(null);
        return super.toString() + ": " + InstanceFactory.instance(JsonParser.class).toJson(cloneEvent);
//        return "TapEvent{" +
//                "time=" + time +
//                ", info=" + info +
//                ", traceMap=" + traceMap +
//                '}';
    }

    public String key() {
        if (null == this.key) {
            this.key = FormatUtils.formatTapEvent(this.getClass());
        }
        return this.key;
    }
}
