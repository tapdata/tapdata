package io.tapdata.pdk.core.utils.cache;

import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ObjectSerializable;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.SerializerException;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class ObjectSerializer implements Serializer<Object> {
        public ObjectSerializer(ClassLoader classLoader) {}
        @Override
        public ByteBuffer serialize(Object object) throws SerializerException {
            return ByteBuffer.wrap(InstanceFactory.instance(ObjectSerializable.class).fromObject(object));
        }

        @Override
        public Object read(ByteBuffer binary) throws ClassNotFoundException, SerializerException {
            if (binary.hasArray()) {
                final byte[] array = binary.array();
                final int arrayOffset = binary.arrayOffset();
                byte[] data = Arrays.copyOfRange(array, arrayOffset + binary.position(),
                        arrayOffset + binary.limit());
                return InstanceFactory.instance(ObjectSerializable.class).toObject(data);
            }
            return null;
        }

        @Override
        public boolean equals(Object object, ByteBuffer binary) throws ClassNotFoundException, SerializerException {
            return false;
        }
    }
