package io.tapdata.flow.engine.V2.node.duckdb.converter;

import com.tapdata.tm.commons.dag.process.dto.TapFieldDto;
import com.tapdata.tm.commons.dag.process.dto.TapTableDto;
import io.tapdata.flow.engine.V2.node.duckdb.NodeSchemaInfo;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class IengineSchemaConverterTest {
    @Test
    void abstractSchemaConverter_cacheLifecycle() {
        AtomicInteger converted = new AtomicInteger();
        DummyConverter converter = new DummyConverter(converted);

        assertNull(converter.convert(null));
        assertEquals("X", converter.convert("x"));
        assertEquals(1, converted.get());

        converter.cache("k", "v");
        assertTrue(converter.isCached("k"));
        converter.clearCache();
        assertFalse(converter.isCached("k"));
    }

    @Test
    void abstractSchemaConverter_tryCleanup_removesExpired() {
        DummyConverter converter = new DummyConverter(new AtomicInteger());
        converter.setCacheExpireTime(Long.MAX_VALUE);
        converter.setCleanupInterval(0);
        converter.cache("k", "v");
        assertTrue(converter.isCached("k"));
        converter.setCacheExpireTime(-1);
        converter.cleanupNow();
        assertFalse(converter.isCached("k"));
    }

    @Test
    void iengineSchemaConverter_getInstance_isSingleton() {
        IengineSchemaConverter c1 = IengineSchemaConverter.getInstance();
        IengineSchemaConverter c2 = IengineSchemaConverter.getInstance();
        assertSame(c1, c2);
    }

    @Test
    void iengineSchemaConverter_convertsTapTableDtoToNodeSchemaInfo() {
        TapTableDto table = new TapTableDto("node1", "users");
        TapFieldDto id = new TapFieldDto()
                .name("id")
                .originalFieldName("id")
                .dataType("BIGINT")
                .primaryKeyPos(1)
                .arrowTypeName("Int")
                .arrowBitWidth(64)
                .duckDbTypeName("BIGINT");
        TapFieldDto name = new TapFieldDto()
                .name("name")
                .originalFieldName("name")
                .dataType("VARCHAR")
                .arrowTypeName("Utf8")
                .duckDbTypeName("VARCHAR");
        table.addField(id).addField(name);
        table.addPrimaryKey("id");

        IengineSchemaConverter converter = IengineSchemaConverter.getInstance();
        Map<String, NodeSchemaInfo> map = converter.convert(List.of(table));

        assertEquals(1, map.size());
        NodeSchemaInfo schema = map.get("node1");
        assertNotNull(schema);
        assertEquals("users", schema.getTableName());
        assertEquals(List.of("id"), schema.getPrimaryKeys());
        assertTrue(schema.hasField("id"));
        assertTrue(schema.isPrimaryKey("id"));
        assertNotNull(schema.getArrowSchema());
        assertEquals(2, schema.getArrowSchema().getFields().size());
        assertTrue(schema.getArrowSchema().getFields().get(0).getType() instanceof ArrowType.Int);
    }

    private static class DummyConverter extends AbstractSchemaConverter<String, String> {
        private final AtomicInteger converted;

        private DummyConverter(AtomicInteger converted) {
            this.converted = converted;
        }

        @Override
        protected String getSourceKey(String source) {
            return source;
        }

        @Override
        protected String doConvert(String source) {
            converted.incrementAndGet();
            return source.toUpperCase();
        }

        void cleanupNow() {
            tryCleanup();
        }
    }
}

