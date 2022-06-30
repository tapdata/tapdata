package io.tapdata.entity.utils;

public interface ObjectSerializable {
    byte[] fromObject(Object obj);
    Object toObject(byte[] data);
    Object toObject(byte[] data, ToObjectOptions options);

    class ToObjectOptions{
        private ClassLoader classLoader;
        public ToObjectOptions classLoader(ClassLoader classLoader) {
            this.classLoader = classLoader;
            return this;
        }

        public ClassLoader getClassLoader() {
            return classLoader;
        }

        public void setClassLoader(ClassLoader classLoader) {
            this.classLoader = classLoader;
        }
    }
}
