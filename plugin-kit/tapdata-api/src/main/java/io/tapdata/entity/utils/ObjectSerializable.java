package io.tapdata.entity.utils;

public interface ObjectSerializable {
    byte[] fromObject(Object obj);

    byte[] fromObject(Object obj, FromObjectOptions options);

    Object toObject(byte[] data);
    Object toObject(byte[] data, ToObjectOptions options);

    class FromObjectOptions {
        private boolean toJavaPlatform = true;
        public FromObjectOptions toJavaPlatform(boolean toJavaPlatform) {
            this.toJavaPlatform = toJavaPlatform;
            return this;
        }

        public boolean isToJavaPlatform() {
            return toJavaPlatform;
        }

        public void setToJavaPlatform(boolean toJavaPlatform) {
            this.toJavaPlatform = toJavaPlatform;
        }
    }

    class ToObjectOptions {
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
