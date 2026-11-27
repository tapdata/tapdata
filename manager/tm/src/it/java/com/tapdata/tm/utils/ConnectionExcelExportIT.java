package com.tapdata.tm.utils;

import cn.hutool.extra.spring.SpringUtil;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

class ConnectionExcelExportIT {

    private final UserDetail user = mock(UserDetail.class);

    @Test
    @DisplayName("单条 JDBC 连接可导出和重新解析，保留可编辑配置及隐藏列")
    void jdbcConnectionRoundTripsThroughXlsx() throws IOException {
        var jdbc = jdbcConnection("测试连接");
        try (var definitions = stubConnectionDefinitions()) {
            byte[] output = ExcelUtil.exportConnectionsToExcel(List.of(jdbc), user);

            try (var workbook = WorkbookFactory.create(new ByteArrayInputStream(output))) {
                assertThat(workbook.getNumberOfSheets()).isEqualTo(1);
                var sheet = workbook.getSheet("JDBC");
                assertThat(sheet.getProtect()).isTrue();
                assertThat(sheet.getRow(0).getCell(2).getStringCellValue()).isEqualTo("database_port");
                assertThat(sheet.getRow(1).getCell(2).getCellStyle().getLocked()).isFalse();
                assertThat(sheet.getRow(0).getCell(3).getStringCellValue()).isEqualTo("id");
                assertThat(sheet.isColumnHidden(3)).isTrue();
            }

            var imported = ExcelUtil.importConnectionsFromExcel(new ByteArrayInputStream(output), user);
            assertThat(imported).singleElement().satisfies(connection -> {
                assertThat(connection.getName()).isEqualTo(jdbc.getName());
                assertThat(connection.getDatabase_type()).isEqualTo("mysql");
                assertThat(connection.getConfig()).containsEntry("port", 3306L);
            });
        }
    }

    @Test
    @DisplayName("混合批量导出包含 Mongo 和 JDBC 工作表，超长中文列宽受 Excel 上限约束")
    void mixedConnectionsRoundTripWithBoundedColumnWidth() throws IOException {
        String longName = "测试连接".repeat(80);
        var jdbc = jdbcConnection(longName);
        var mongo = new DataSourceConnectionDto();
        mongo.setName("Mongo 连接");
        mongo.setDatabase_type("mongodb");

        try (var definitions = stubConnectionDefinitions()) {
            byte[] output = ExcelUtil.exportConnectionsToExcel(List.of(jdbc, mongo), user);

            try (var workbook = WorkbookFactory.create(new ByteArrayInputStream(output))) {
                assertThat(workbook.getNumberOfSheets()).isEqualTo(2);
                assertThat(workbook.getSheet("JDBC").getColumnWidth(0)).isEqualTo(255 * 256);
                assertThat(workbook.getSheet("Mongo").getRow(1).getCell(0).getStringCellValue())
                        .isEqualTo(mongo.getName());
            }

            var imported = ExcelUtil.importConnectionsFromExcel(new ByteArrayInputStream(output), user);
            assertThat(imported).extracting(DataSourceConnectionDto::getName)
                    .containsExactly(mongo.getName(), longName);
            assertThat(imported.get(1).getConfig()).containsEntry("port", 3306L);
        }
    }

    private static DataSourceConnectionDto jdbcConnection(String name) {
        var connection = new DataSourceConnectionDto();
        connection.setName(name);
        connection.setDatabase_type("mysql");
        connection.setPdkHash("mysql-test");
        connection.setConfig(Map.of("port", 3306));
        return connection;
    }

    private MockedStatic<SpringUtil> stubConnectionDefinitions() {
        var definition = new DataSourceDefinitionDto();
        definition.setPdkHash("mysql-test");
        definition.setProperties(new LinkedHashMap<>(Map.of("connection", Map.of(
                "properties", Map.of("port", Map.of("apiServerKey", "database_port"))))));
        var service = mock(DataSourceDefinitionService.class);
        when(service.getByDataSourceType(anyList(), same(user))).thenReturn(List.of(definition));
        var spring = mockStatic(SpringUtil.class);
        spring.when(() -> SpringUtil.getBean(DataSourceDefinitionService.class)).thenReturn(service);
        return spring;
    }
}
