package io.tapdata.entity.utils;

public interface ObjectSerializable {
    byte[] fromObject(Object obj);

    byte[] fromObject(Object obj, FromObjectOptions options);

    Object toObject(byte[] data);
    Object toObject(byte[] data, ToObjectOptions options);

    class FromObjectOptions {
        private boolean writeNullValue = true;
        public FromObjectOptions writeNullValue(boolean writeNullValue) {
            this.writeNullValue = writeNullValue;
            return this;
        }
        private boolean useActualMapAndList = true;
        public FromObjectOptions useActualMapAndList(boolean useActualMapAndList) {
            this.useActualMapAndList = useActualMapAndList;
            return this;
        }
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

        public boolean isUseActualMapAndList() {
            return useActualMapAndList;
        }

        public void setUseActualMapAndList(boolean useActualMapAndList) {
            this.useActualMapAndList = useActualMapAndList;
        }

        public boolean isWriteNullValue() {
            return writeNullValue;
        }

        public void setWriteNullValue(boolean writeNullValue) {
            this.writeNullValue = writeNullValue;
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
