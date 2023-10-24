package io.tapdata.pdk.apis.context;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.utils.DefaultConcurrentMap;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;

import java.util.UUID;


public class TapContext {
    protected String id;
    protected TapNodeSpecification specification;

    protected final DefaultConcurrentMap attributes = new DefaultConcurrentMap();
    protected Log log;

    public TapContext(TapNodeSpecification specification) {
        this.specification = specification;
        id = UUID.randomUUID().toString().replace("-", "");
    }

    public Object putAttributeIfAbsent(String key, Object value) {
        return attributes.putIfAbsent(key, value);
    }

    public Object putAttribute(String key, Object value) {
        return attributes.put(key, value);
    }

    public Object removeAttribute(String key) {
        return attributes.remove(key);
    }

    public TapNodeSpecification getSpecification() {
        return specification;
    }

    public void setSpecification(TapNodeSpecification specification) {
        this.specification = specification;
    }

    public String getId() {
        return id;
    }


    public Log getLog() {
        return log;
    }

    public void setLog(Log log) {
        this.log = log;
    }
}
