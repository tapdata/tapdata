package io.tapdata.factory;

public enum InspectType {
    INSPECT_SERVICE_IMPL("io.tapdata.inspect.InspectServiceImpl");
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
