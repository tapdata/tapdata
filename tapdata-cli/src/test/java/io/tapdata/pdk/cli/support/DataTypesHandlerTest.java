package io.tapdata.pdk.cli.support;

import io.tapdata.entity.mapping.type.TapMapping;
import io.tapdata.entity.schema.TapTable;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DataTypesHandlerTest {
    @Test
    void coefficientMappingDoesNotGenerateAnUnresolvedTargetType() {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("to", "TapCoefficientFloat");
        info.put("precision", Arrays.asList(1, 53));
        Map<String, Object> coefficient = new LinkedHashMap<>();
        coefficient.put("TapFloat", Arrays.asList(1, 24));
        info.put("coefficient", coefficient);

        TapMapping mapping = TapMapping.build(info);
        TapTable table = new TapTable();

        DataTypesHandler.create().fillTestFields(table, "float($precision)", mapping);

        assertEquals(1, table.getNameFieldMap().size());
        assertEquals("float(24)", table.getNameFieldMap().values().iterator().next().getDataType());
    }
}
