package io.tapdata.inspect;

public enum InspectType {
    INSPECT_SERVICE_IMPL("io.tapdata.inspect.InspectServiceImpl",new Class[]{Object.class});
    private String clazz;
    private Class<?>[] classes;

    InspectType(String clazz) {
        this.clazz = clazz;
    }

    InspectType(String clazz, Class<?>[] classes) {
        this.clazz = clazz;
        this.classes = classes;
    }

    public String getClazz() {
        return clazz;
    }

    public Class<?>[] getClasses() {
        return classes;
    }
}
