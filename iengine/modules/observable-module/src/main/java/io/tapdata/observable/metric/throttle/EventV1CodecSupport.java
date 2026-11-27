package io.tapdata.observable.metric.throttle;

import io.tapdata.entity.schema.value.ByteData;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapValue;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Shared canonical traversal for event-v1 byte serialization and zero-allocation length counting.
 * Guarantees exact byte parity between {@link EventV1ReferenceSerializer} and {@link EventCostMeter}.
 */
final class EventV1CodecSupport {

    static final int DEFAULT_MAX_DEPTH = 64;
    static final int DEFAULT_MAX_WORK_UNITS = 65_536;

    private static final byte[] HEX_LOWER = "0123456789abcdef".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] KEY_TYPE = "\"type\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] KEY_TABLE = ",\"table\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] KEY_EVENT_TIME = ",\"eventTimeMs\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] KEY_BEFORE = ",\"before\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] KEY_AFTER = ",\"after\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] TYPE_TAG_PREFIX = "{\"$t\":\"".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] TYPE_VAL_PREFIX = "\",\"v\":\"".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] TYPE_OBJ_END = "\"}".getBytes(StandardCharsets.US_ASCII);

    private EventV1CodecSupport() {
    }

    static void emit(EventV1View view, Sink sink, boolean sortMapKeys, int maxDepth, int maxWorkUnits) {
        if (view == null) {
            throw new IllegalArgumentException("EventV1View must not be null");
        }
        State state = new State(sink, sortMapKeys, maxDepth, maxWorkUnits);
        state.chargeWork(1);
        sink.writeByte((byte) '{');
        sink.writeAscii(KEY_TYPE);
        state.writeJsonString(view.getType().name());
        sink.writeAscii(KEY_TABLE);
        state.writeJsonString(view.getTable());
        sink.writeAscii(KEY_EVENT_TIME);
        sink.writeAscii(Long.toString(view.getEventTimeMs()));
        sink.writeAscii(KEY_BEFORE);
        state.writeMapOrNull(view.getBefore(), 1);
        sink.writeAscii(KEY_AFTER);
        state.writeMapOrNull(view.getAfter(), 1);
        sink.writeByte((byte) '}');
    }

    interface Sink {
        void writeByte(byte b);

        void writeAscii(String s);

        void writeAscii(byte[] asciiBytes);

        void writeJsonString(CharSequence value);

        void writeBase64(byte[] bytes, int offset, int length);
    }

    static final class ByteArraySink implements Sink {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream(256);

        @Override
        public void writeByte(byte b) {
            out.write(b);
        }

        @Override
        public void writeAscii(String s) {
            for (int i = 0; i < s.length(); i++) {
                out.write(s.charAt(i));
            }
        }

        @Override
        public void writeAscii(byte[] asciiBytes) {
            out.write(asciiBytes, 0, asciiBytes.length);
        }

        @Override
        public void writeJsonString(CharSequence value) {
            out.write('"');
            int len = value.length();
            for (int i = 0; i < len; i++) {
                char c = value.charAt(i);
                switch (c) {
                    case '"' -> { out.write('\\'); out.write('"'); }
                    case '\\' -> { out.write('\\'); out.write('\\'); }
                    case '\b' -> { out.write('\\'); out.write('b'); }
                    case '\f' -> { out.write('\\'); out.write('f'); }
                    case '\n' -> { out.write('\\'); out.write('n'); }
                    case '\r' -> { out.write('\\'); out.write('r'); }
                    case '\t' -> { out.write('\\'); out.write('t'); }
                    default -> {
                        if (c < 0x20) {
                            out.write('\\');
                            out.write('u');
                            out.write('0');
                            out.write('0');
                            out.write(HEX_LOWER[(c >>> 4) & 0xF]);
                            out.write(HEX_LOWER[c & 0xF]);
                        } else if (c < 0x80) {
                            out.write(c);
                        } else if (c < 0x800) {
                            out.write(0xC0 | (c >>> 6));
                            out.write(0x80 | (c & 0x3F));
                        } else if (Character.isHighSurrogate(c) && i + 1 < len && Character.isLowSurrogate(value.charAt(i + 1))) {
                            int cp = Character.toCodePoint(c, value.charAt(++i));
                            out.write(0xF0 | (cp >>> 18));
                            out.write(0x80 | ((cp >>> 12) & 0x3F));
                            out.write(0x80 | ((cp >>> 6) & 0x3F));
                            out.write(0x80 | (cp & 0x3F));
                        } else if (Character.isSurrogate(c)) {
                            out.write('?');
                        } else {
                            out.write(0xE0 | (c >>> 12));
                            out.write(0x80 | ((c >>> 6) & 0x3F));
                            out.write(0x80 | (c & 0x3F));
                        }
                    }
                }
            }
            out.write('"');
        }

        @Override
        public void writeBase64(byte[] bytes, int offset, int length) {
            if (length <= 0) {
                return;
            }
            byte[] slice = (offset == 0 && length == bytes.length)
                    ? bytes
                    : Arrays.copyOfRange(bytes, offset, offset + length);
            byte[] encoded = Base64.getEncoder().encode(slice);
            out.write(encoded, 0, encoded.length);
        }

        byte[] toByteArray() {
            return out.toByteArray();
        }
    }

    static final class CountingSink implements Sink {
        private long count;

        @Override
        public void writeByte(byte b) {
            count += 1L;
        }

        @Override
        public void writeAscii(String s) {
            count += s.length();
        }

        @Override
        public void writeAscii(byte[] asciiBytes) {
            count += asciiBytes.length;
        }

        @Override
        public void writeJsonString(CharSequence value) {
            long bytes = 2L; // opening and closing quotes
            int len = value.length();
            for (int i = 0; i < len; i++) {
                char c = value.charAt(i);
                switch (c) {
                    case '"', '\\', '\b', '\f', '\n', '\r', '\t' -> bytes += 2L;
                    default -> {
                        if (c < 0x20) {
                            bytes += 6L;
                        } else if (c < 0x80) {
                            bytes += 1L;
                        } else if (c < 0x800) {
                            bytes += 2L;
                        } else if (Character.isHighSurrogate(c) && i + 1 < len && Character.isLowSurrogate(value.charAt(i + 1))) {
                            i++;
                            bytes += 4L;
                        } else if (Character.isSurrogate(c)) {
                            bytes += 1L;
                        } else {
                            bytes += 3L;
                        }
                    }
                }
            }
            count += bytes;
        }

        @Override
        public void writeBase64(byte[] bytes, int offset, int length) {
            if (length <= 0) {
                return;
            }
            count += 4L * ((length + 2L) / 3L);
        }

        long getCount() {
            return count;
        }
    }

    private static final class State {
        private final Sink sink;
        private final boolean sortMapKeys;
        private final int maxDepth;
        private final int maxWorkUnits;
        private final IdentityHashMap<Object, Boolean> activeStack = new IdentityHashMap<>();
        private int workUnits;

        private State(Sink sink, boolean sortMapKeys, int maxDepth, int maxWorkUnits) {
            this.sink = sink;
            this.sortMapKeys = sortMapKeys;
            this.maxDepth = maxDepth;
            this.maxWorkUnits = maxWorkUnits;
        }

        void chargeWork(int delta) {
            workUnits += delta;
            if (workUnits > maxWorkUnits) {
                throw new MeterBudgetExceededException("Event-v1 metering exceeded maxWorkUnits=" + maxWorkUnits);
            }
        }

        void checkDepth(int depth) {
            if (depth > maxDepth) {
                throw new MeterBudgetExceededException("Event-v1 metering exceeded maxDepth=" + maxDepth);
            }
        }

        void writeJsonString(CharSequence s) {
            int len = s.length();
            if (len >= 4096) {
                chargeWork(len / 4096);
            }
            sink.writeJsonString(s);
        }

        void writeTypedTag(String tag, CharSequence rawValue) {
            sink.writeAscii(TYPE_TAG_PREFIX);
            writeJsonStringUnquoted(tag);
            sink.writeAscii(TYPE_VAL_PREFIX);
            writeJsonStringUnquoted(rawValue);
            sink.writeAscii(TYPE_OBJ_END);
        }

        private void writeJsonStringUnquoted(CharSequence s) {
            // Compute/write by reusing writeJsonString minus quotes? Instead write directly for exact byte count.
            if (sink instanceof CountingSink counting) {
                long before = counting.count;
                sink.writeJsonString(s);
                counting.count -= 2L;
                if (counting.count < before) {
                    counting.count = before;
                }
                return;
            }
            ByteArraySink temp = new ByteArraySink();
            temp.writeJsonString(s);
            byte[] quoted = temp.toByteArray();
            if (quoted.length > 2) {
                sink.writeAscii(Arrays.copyOfRange(quoted, 1, quoted.length - 1));
            }
        }

        void writeMapOrNull(Map<?, ?> map, int depth) {
            if (map == null) {
                sink.writeAscii("null");
                return;
            }
            writeMap(map, depth);
        }

        void writeValue(Object value, int depth) {
            chargeWork(1);
            checkDepth(depth);
            if (value == null) {
                sink.writeAscii("null");
                return;
            }
            if (value instanceof TapValue<?, ?> tapValue) {
                Object inner = tapValue.getValue() != null ? tapValue.getValue() : tapValue.getOriginValue();
                if (inner == tapValue) {
                    writeTypedTag("other", tapValue.getClass().getSimpleName());
                    return;
                }
                writeValue(inner, depth + 1);
                return;
            }
            if (value instanceof CharSequence cs) {
                writeJsonString(cs);
                return;
            }
            if (value instanceof Boolean b) {
                sink.writeAscii(b ? "true" : "false");
                return;
            }
            if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
                sink.writeAscii(Long.toString(((Number) value).longValue()));
                return;
            }
            if (value instanceof BigInteger bi) {
                sink.writeAscii(bi.toString());
                return;
            }
            if (value instanceof BigDecimal bd) {
                sink.writeAscii(bd.toPlainString());
                return;
            }
            if (value instanceof Double d) {
                if (Double.isFinite(d)) {
                    double normalized = d == 0.0d ? 0.0d : d;
                    sink.writeAscii(Double.toString(normalized));
                } else {
                    writeTypedTag("float", Double.toString(d));
                }
                return;
            }
            if (value instanceof Float f) {
                if (Float.isFinite(f)) {
                    float normalized = f == 0.0f ? 0.0f : f;
                    sink.writeAscii(Float.toString(normalized));
                } else {
                    writeTypedTag("float", Float.toString(f));
                }
                return;
            }
            if (value instanceof Number num) {
                sink.writeAscii(num.toString());
                return;
            }
            if (value instanceof byte[] bytes) {
                writeBytesValue(bytes, 0, bytes.length);
                return;
            }
            if (value instanceof ByteData byteData) {
                byte[] bytes = byteData.getValue();
                if (bytes == null) {
                    sink.writeAscii("null");
                } else {
                    writeBytesValue(bytes, 0, bytes.length);
                }
                return;
            }
            if (value instanceof Binary binary) {
                byte[] bytes = binary.getData();
                if (bytes == null) {
                    sink.writeAscii("null");
                } else {
                    writeBytesValue(bytes, 0, bytes.length);
                }
                return;
            }
            if (value instanceof ByteBuffer buf) {
                ByteBuffer dup = buf.asReadOnlyBuffer();
                int remaining = dup.remaining();
                if (remaining >= 4096) {
                    chargeWork(remaining / 4096);
                }
                byte[] bytes = new byte[remaining];
                dup.get(bytes);
                writeBytesValue(bytes, 0, bytes.length);
                return;
            }
            if (value instanceof DateTime dt) {
                writeTapDateTime(dt);
                return;
            }
            if (value instanceof Instant instant) {
                writeTypedTag("datetime", instant.toString());
                return;
            }
            if (value instanceof Timestamp ts) {
                writeTypedTag("datetime", ts.toInstant().toString());
                return;
            }
            if (value instanceof Date date) {
                writeTypedTag("datetime", Instant.ofEpochMilli(date.getTime()).toString());
                return;
            }
            if (value instanceof ZonedDateTime zdt) {
                writeTypedTag("datetime", zdt.toInstant().toString());
                return;
            }
            if (value instanceof OffsetDateTime odt) {
                writeTypedTag("datetime", odt.toInstant().toString());
                return;
            }
            if (value instanceof LocalDate ld) {
                writeTypedTag("date", ld.toString());
                return;
            }
            if (value instanceof LocalTime lt) {
                writeTypedTag("time", lt.toString());
                return;
            }
            if (value instanceof LocalDateTime ldt) {
                writeTypedTag("localDatetime", ldt.toString());
                return;
            }
            if (value instanceof ObjectId oid) {
                writeTypedTag("oid", oid.toHexString());
                return;
            }
            if (value instanceof UUID uuid) {
                writeTypedTag("uuid", uuid.toString());
                return;
            }
            if (value instanceof Map<?, ?> map) {
                writeMap(map, depth);
                return;
            }
            if (value instanceof Collection<?> collection) {
                writeCollection(collection, depth);
                return;
            }
            if (value.getClass().isArray()) {
                writeArray(value, depth);
                return;
            }
            writeTypedTag("other", value.toString());
        }

        private void writeBytesValue(byte[] bytes, int offset, int length) {
            if (length >= 4096) {
                chargeWork(length / 4096);
            }
            sink.writeAscii(TYPE_TAG_PREFIX);
            sink.writeAscii("bytes");
            sink.writeAscii(TYPE_VAL_PREFIX);
            sink.writeBase64(bytes, offset, length);
            sink.writeAscii(TYPE_OBJ_END);
        }

        private void writeTapDateTime(DateTime dt) {
            if (dt.isContainsIllegal() && dt.getIllegalDate() != null) {
                writeTypedTag("datetime", dt.getIllegalDate());
                return;
            }
            if (dt.getSeconds() != null) {
                long nano = dt.getNano() != null ? dt.getNano() : 0L;
                writeTypedTag("datetime", Instant.ofEpochSecond(dt.getSeconds(), nano).toString());
                return;
            }
            if (dt.getOriginBytes() != null) {
                writeBytesValue(dt.getOriginBytes(), 0, dt.getOriginBytes().length);
                return;
            }
            writeTypedTag("datetime", "");
        }

        private void enterComposite(Object composite) {
            if (activeStack.put(composite, Boolean.TRUE) != null) {
                throw new MeterBudgetExceededException("Circular reference detected during event-v1 metering");
            }
        }

        private void exitComposite(Object composite) {
            activeStack.remove(composite);
        }

        private void writeMap(Map<?, ?> map, int depth) {
            checkDepth(depth);
            enterComposite(map);
            try {
                sink.writeByte((byte) '{');
                if (sortMapKeys) {
                    List<Map.Entry<String, Object>> sorted = new ArrayList<>(map.size());
                    for (Map.Entry<?, ?> entry : map.entrySet()) {
                        String keyStr = entry.getKey() == null ? "null" : String.valueOf(entry.getKey());
                        sorted.add(new AbstractMap.SimpleImmutableEntry<>(keyStr, entry.getValue()));
                    }
                    sorted.sort((a, b) -> compareUtf8Unsigned(a.getKey(), b.getKey()));
                    boolean first = true;
                    for (Map.Entry<String, Object> entry : sorted) {
                        if (!first) {
                            sink.writeByte((byte) ',');
                        }
                        first = false;
                        writeJsonString(entry.getKey());
                        sink.writeByte((byte) ':');
                        writeValue(entry.getValue(), depth + 1);
                    }
                } else {
                    boolean first = true;
                    for (Map.Entry<?, ?> entry : map.entrySet()) {
                        if (!first) {
                            sink.writeByte((byte) ',');
                        }
                        first = false;
                        String keyStr = entry.getKey() == null ? "null" : String.valueOf(entry.getKey());
                        writeJsonString(keyStr);
                        sink.writeByte((byte) ':');
                        writeValue(entry.getValue(), depth + 1);
                    }
                }
                sink.writeByte((byte) '}');
            } finally {
                exitComposite(map);
            }
        }

        private void writeCollection(Collection<?> collection, int depth) {
            checkDepth(depth);
            enterComposite(collection);
            try {
                sink.writeByte((byte) '[');
                boolean first = true;
                for (Object item : collection) {
                    if (!first) {
                        sink.writeByte((byte) ',');
                    }
                    first = false;
                    writeValue(item, depth + 1);
                }
                sink.writeByte((byte) ']');
            } finally {
                exitComposite(collection);
            }
        }

        private void writeArray(Object array, int depth) {
            checkDepth(depth);
            enterComposite(array);
            try {
                sink.writeByte((byte) '[');
                int len = Array.getLength(array);
                for (int i = 0; i < len; i++) {
                    if (i > 0) {
                        sink.writeByte((byte) ',');
                    }
                    writeValue(Array.get(array, i), depth + 1);
                }
                sink.writeByte((byte) ']');
            } finally {
                exitComposite(array);
            }
        }

        private static int compareUtf8Unsigned(String a, String b) {
            byte[] ba = a.getBytes(StandardCharsets.UTF_8);
            byte[] bb = b.getBytes(StandardCharsets.UTF_8);
            return Arrays.compareUnsigned(ba, bb);
        }
    }
}
