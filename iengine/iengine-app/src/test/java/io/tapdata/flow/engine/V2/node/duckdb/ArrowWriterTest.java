package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.tm.commons.dag.process.dto.TapFieldDto;
import com.tapdata.tm.commons.dag.process.dto.TapTableDto;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.observable.logging.ObsLogger;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class ArrowWriterTest {
    io.tapdata.observable.logging.ObsLogger logger;
    @BeforeEach
    void init() {
        logger = mock(io.tapdata.observable.logging.ObsLogger.class);
    }

    @Test
    void writeWithArrowCoversPublicOverloadsAndFallbackInsertions() throws Exception {
        TapTable tapTable = tapTable(
                tapField("id", "BIGINT", false, true, 1, 1),
                tapField("name", "VARCHAR", true, false, null, 2)
        );
        TapTableDto tapTableDto = tapTableDto(
                tapFieldDto("id", "BIGINT", false, true, 1, 1),
                tapFieldDto("name", "VARCHAR", true, false, 0, 2)
        );
        NodeSchemaInfo schemaInfo = nodeSchemaInfo("schema_target", tapTable);
        List<Map<String, Object>> data = List.of(
                row(1L, "alpha"),
                row(2L, "beta")
        );

        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = connection.createStatement();
             ArrowWriter writer = new ArrowWriter(connection, false, DuckLakeConfig.disabled()).log(logger)) {
            statement.executeUpdate("CREATE TABLE tap_table_target(id BIGINT PRIMARY KEY, name VARCHAR)");
            statement.executeUpdate("CREATE TABLE dto_target(id BIGINT PRIMARY KEY, name VARCHAR)");
            statement.executeUpdate("CREATE TABLE schema_target(id BIGINT PRIMARY KEY, name VARCHAR)");

            writer.writeWithArrow(data, "tap_table_target", tapTable);
            writer.writeWithArrow(data, "dto_target", tapTableDto);
            writer.writeWithArrow(data, schemaInfo);

            assertEquals(2L, queryCount(connection, "tap_table_target"));
            assertEquals(2L, queryCount(connection, "dto_target"));
            assertEquals(2L, queryCount(connection, "schema_target"));
        }

        CopyTrackingArrowWriter routingWriter =(CopyTrackingArrowWriter) new CopyTrackingArrowWriter(mock(Connection.class), true, DuckLakeConfig.disabled()).log(logger);
        routingWriter.copyResult = true;
        routingWriter.writeWithArrow(data, "copy_only", tapTable);
        routingWriter.writeWithArrow(data, "copy_dto", tapTableDto);
        routingWriter.writeWithArrow(data, schemaInfo);
        assertEquals(3, routingWriter.copyCalls);

        routingWriter.writeWithArrow(Collections.emptyList(), "copy_only", tapTable);
        routingWriter.writeWithArrow(Collections.emptyList(), "copy_only", tapTableDto);
        routingWriter.writeWithArrow(Collections.emptyList(), schemaInfo);
        assertEquals(3, routingWriter.copyCalls);
        routingWriter.close();
    }

    @Test
    void writeWithArrowRoutesDuckLakeAndRegularBranches() throws Exception {
        TapTable tapTable = tapTable(
                tapField("id", "BIGINT", false, true, 1, 1),
                tapField("name", "VARCHAR", true, false, null, 2)
        );
        TapTableDto tapTableDto = tapTableDto(
                tapFieldDto("id", "BIGINT", false, true, 1, 1),
                tapFieldDto("name", "VARCHAR", true, false, 0, 2)
        );
        NodeSchemaInfo schemaInfo = nodeSchemaInfo("ducklake_schema", tapTable);
        List<Map<String, Object>> data = List.of(row(1L, "duck"));

        Connection duckLakeConnection = mock(Connection.class);
        Statement createStatement = mock(Statement.class);
        when(duckLakeConnection.createStatement()).thenReturn(createStatement);
        when(createStatement.execute(anyString())).thenReturn(true);

        RoutingArrowWriter duckLakeWriter =(RoutingArrowWriter) new RoutingArrowWriter(
                duckLakeConnection,
                false,
                new DuckLakeConfig(true, "LOCAL", "/tmp/duck'lake", null)
        ).log(logger);
        duckLakeWriter.writeWithArrow(data, "duck_table", tapTable, true);
        duckLakeWriter.writeWithArrow(data, "duck_table_dto", tapTableDto, true);
        duckLakeWriter.writeWithArrow(data, schemaInfo, true);
        duckLakeWriter.writeWithArrow(data, "duck_table_false", tapTable, false);
        duckLakeWriter.writeWithArrow(data, "duck_table_dto_false", tapTableDto, false);
        duckLakeWriter.writeWithArrow(data, schemaInfo, false);

        assertEquals(2, duckLakeWriter.tapTableWrites);
        assertEquals(2, duckLakeWriter.tapTableDtoWrites);
        assertEquals(2, duckLakeWriter.schemaInfoWrites);
        verify(createStatement, org.mockito.Mockito.times(3)).execute(contains("LOCAL = '/tmp/duck''lake'"));

        duckLakeWriter.writeWithArrow(Collections.emptyList(), "duck_table", tapTable, true);
        duckLakeWriter.writeWithArrow(Collections.emptyList(), "duck_table_dto", tapTableDto, true);
        duckLakeWriter.writeWithArrow(Collections.emptyList(), schemaInfo, true);
        assertEquals(2, duckLakeWriter.tapTableWrites);
        assertEquals(2, duckLakeWriter.tapTableDtoWrites);
        assertEquals(2, duckLakeWriter.schemaInfoWrites);
        duckLakeWriter.close();

        RoutingArrowWriter regularWriter = (RoutingArrowWriter) new RoutingArrowWriter(mock(Connection.class), false, DuckLakeConfig.disabled()).log(logger);
        regularWriter.writeWithArrow(data, "regular_table", tapTable, true);
        regularWriter.writeWithArrow(data, "regular_table_dto", tapTableDto, true);
        regularWriter.writeWithArrow(data, schemaInfo, true);

        assertEquals(1, regularWriter.tapTableWrites);
        assertEquals(1, regularWriter.tapTableDtoWrites);
        assertEquals(1, regularWriter.schemaInfoWrites);
        regularWriter.close();
    }

    @Test
    void privateInsertHelpersCoverFallbackRollbackAndIgnoredDuplicates() throws Throwable {
        TapTable tapTableWithPk = tapTable(
                tapField("id", "BIGINT", false, true, 1, 1),
                tapField("name", "VARCHAR", true, false, null, 2)
        );
        TapTable tapTableWithCompositePk = tapTable(
                tapField("id", "BIGINT", false, true, 1, 1),
                tapField("name", "VARCHAR", false, true, 2, 2)
        );
        TapTable tapTableWithoutPk = tapTable(
                tapField("id", "BIGINT", null, false, null, 1),
                tapField("name", "VARCHAR", true, false, null, 2)
        );
        TapTable tapTableWithoutFields = new TapTable();
        tapTableWithoutFields.setNameFieldMap(new LinkedHashMap<>());
        List<Map<String, Object>> data = List.of(
                row(1L, "a"),
                row(1L, "duplicate")
        );

        Connection emptyAppenderConnection = mock(Connection.class);
        try (ArrowWriter writer = new ArrowWriter(emptyAppenderConnection, false, DuckLakeConfig.disabled()).log(logger)) {
            invoke(writer, "AppenderInsert",
                    new Class[]{List.class, String.class, TapTable.class},
                    Collections.emptyList(), "ignored", tapTableWithPk);
            invoke(writer, "AppenderInsert",
                    new Class[]{List.class, String.class, TapTable.class},
                    data, "ignored", tapTableWithoutFields);
            verifyNoInteractions(emptyAppenderConnection);
        }

        Connection fallbackConnection = mock(Connection.class);
        Statement fallbackStatement = mock(Statement.class);
        when(fallbackConnection.unwrap(DuckDBConnection.class)).thenReturn(null);
        when(fallbackConnection.createStatement()).thenReturn(fallbackStatement);
        when(fallbackConnection.getAutoCommit()).thenReturn(false).thenThrow(new SQLException("state"));
        when(fallbackStatement.executeUpdate(anyString())).thenReturn(2);
        try (ArrowWriter writer = new ArrowWriter(fallbackConnection, false, DuckLakeConfig.disabled()).log(logger)) {
            invoke(writer, "AppenderInsert",
                    new Class[]{List.class, String.class, TapTable.class},
                    data, "fallback_target", tapTableWithPk);
            verify(fallbackConnection).setAutoCommit(false);
            verify(fallbackConnection).commit();
            verify(fallbackConnection).setAutoCommit(true);
            verify(fallbackStatement).executeUpdate(contains("ON CONFLICT"));
        }

        Connection autoCommitTrueConnection = mock(Connection.class);
        Statement autoCommitTrueStatement = mock(Statement.class);
        when(autoCommitTrueConnection.unwrap(DuckDBConnection.class)).thenReturn(null);
        when(autoCommitTrueConnection.createStatement()).thenReturn(autoCommitTrueStatement);
        when(autoCommitTrueConnection.getAutoCommit()).thenReturn(true);
        when(autoCommitTrueStatement.executeUpdate(anyString())).thenReturn(1);
        try (ArrowWriter writer = new ArrowWriter(autoCommitTrueConnection, false, DuckLakeConfig.disabled()).log(logger)) {
            invoke(writer, "AppenderInsert",
                    new Class[]{List.class, String.class, TapTable.class},
                    List.of(row(9L, "auto")), "auto_commit_target", tapTableWithPk);
            verify(autoCommitTrueConnection, never()).setAutoCommit(true);
        }

        Connection resetFailureConnection = mock(Connection.class);
        Statement resetFailureStatement = mock(Statement.class);
        when(resetFailureConnection.unwrap(DuckDBConnection.class)).thenReturn(null);
        when(resetFailureConnection.createStatement()).thenReturn(resetFailureStatement);
        when(resetFailureConnection.getAutoCommit()).thenThrow(new SQLException("cannot read auto commit"));
        when(resetFailureStatement.executeUpdate(anyString())).thenReturn(1);
        try (ArrowWriter writer = new ArrowWriter(resetFailureConnection, false, DuckLakeConfig.disabled()).log(logger)) {
            invoke(writer, "AppenderInsert",
                    new Class[]{List.class, String.class, TapTable.class},
                    List.of(row(10L, "reset")), "reset_failure_target", tapTableWithPk);
        }

        Connection rollbackConnection = mock(Connection.class);
        DuckDBConnection duckDBConnection = mock(DuckDBConnection.class);
        Statement rollbackStatement = mock(Statement.class);
        when(rollbackConnection.unwrap(DuckDBConnection.class)).thenReturn(duckDBConnection);
        when(rollbackConnection.createStatement()).thenReturn(rollbackStatement);
        when(rollbackConnection.getAutoCommit()).thenReturn(false);
        when(duckDBConnection.createAppender(anyString(), anyString())).thenThrow(new SQLException("append failed"));
        when(rollbackStatement.executeUpdate(anyString())).thenReturn(1);
        try (ArrowWriter writer = new ArrowWriter(rollbackConnection, false, DuckLakeConfig.disabled()).log(logger)) {
            invoke(writer, "AppenderInsert",
                    new Class[]{List.class, String.class, TapTable.class},
                    List.of(row(2L, "b")), "rollback_target", tapTableWithPk);
            verify(rollbackConnection).rollback();
            verify(rollbackConnection).commit();
            verify(rollbackStatement).executeUpdate(anyString());
        }

        Connection rowByRowConnection = mock(Connection.class);
        Statement batchStatement = mock(Statement.class);
        Statement firstRowStatement = mock(Statement.class);
        Statement upsertRowStatement = mock(Statement.class);
        when(rowByRowConnection.createStatement()).thenReturn(batchStatement, firstRowStatement, upsertRowStatement);
        when(batchStatement.executeUpdate(anyString())).thenThrow(new SQLException("batch failed"));
        when(firstRowStatement.executeUpdate(anyString())).thenReturn(1);
        when(upsertRowStatement.executeUpdate(anyString())).thenReturn(1);
        try (ArrowWriter writer = new ArrowWriter(rowByRowConnection, false, DuckLakeConfig.disabled()).log(logger)) {
            invoke(writer, "fallbackInsert",
                    new Class[]{List.class, String.class, TapTable.class},
                    data, "row_by_row_target", tapTableWithPk);
            verify(batchStatement).executeUpdate(contains("ON CONFLICT"));
            verify(firstRowStatement).executeUpdate(contains("ON CONFLICT (\"id\") DO UPDATE SET \"name\" = EXCLUDED.\"name\""));
            verify(upsertRowStatement).executeUpdate(contains("ON CONFLICT (\"id\") DO UPDATE SET \"name\" = EXCLUDED.\"name\""));
        }

        Connection noPkConnection = mock(Connection.class);
        Statement noPkStatement = mock(Statement.class);
        when(noPkConnection.createStatement()).thenReturn(noPkStatement);
        when(noPkStatement.executeUpdate(anyString())).thenReturn(2);
        try (ArrowWriter writer = new ArrowWriter(noPkConnection, false, DuckLakeConfig.disabled()).log(logger)) {
            invoke(writer, "fallbackInsert",
                    new Class[]{List.class, String.class, TapTable.class},
                    data, "no_pk_target", tapTableWithoutPk);
            invoke(writer, "fallbackInsert",
                    new Class[]{List.class, String.class, TapTable.class},
                    Collections.emptyList(), "ignored", tapTableWithPk);
            invoke(writer, "fallbackInsert",
                    new Class[]{List.class, String.class, TapTable.class},
                    data, "ignored", tapTableWithoutFields);
            verify(noPkStatement).executeUpdate(contains("INSERT INTO no_pk_target"));
        }

        Connection compositePkConnection = mock(Connection.class);
        Statement compositePkStatement = mock(Statement.class);
        when(compositePkConnection.createStatement()).thenReturn(compositePkStatement);
        when(compositePkStatement.executeUpdate(anyString())).thenReturn(2);
        try (ArrowWriter writer = new ArrowWriter(compositePkConnection, false, DuckLakeConfig.disabled()).log(logger)) {
            invoke(writer, "fallbackInsert",
                    new Class[]{List.class, String.class, TapTable.class},
                    List.of(row(11L, "composite")),
                    "composite_pk_target",
                    tapTableWithCompositePk);
            verify(compositePkStatement).executeUpdate(contains("ON CONFLICT (\"id\", \"name\") DO NOTHING"));
        }

        Connection rethrowConnection = mock(Connection.class);
        Statement brokenRowStatement = mock(Statement.class);
        when(rethrowConnection.createStatement()).thenReturn(brokenRowStatement);
        when(brokenRowStatement.executeUpdate(anyString())).thenThrow(new SQLException("other failure"));
        try (ArrowWriter writer = new ArrowWriter(rethrowConnection, false, DuckLakeConfig.disabled()).log(logger)) {
            SQLException exception = assertThrows(SQLException.class, () -> invoke(
                    writer,
                    "insertRowByRow",
                    new Class[]{List.class, String.class, TapTable.class, List.class},
                    List.of(row(3L, "c")),
                    "rethrow_target",
                    tapTableWithPk,
                    List.of("id", "name")
            ));
            assertEquals("other failure", exception.getMessage());
        }

        Connection pkOnlyConnection = mock(Connection.class);
        Statement pkOnlyStatement = mock(Statement.class);
        when(pkOnlyConnection.createStatement()).thenReturn(pkOnlyStatement);
        when(pkOnlyStatement.executeUpdate(anyString())).thenReturn(0);
        try (ArrowWriter writer = new ArrowWriter(pkOnlyConnection, false, DuckLakeConfig.disabled()).log(logger)) {
            TapTable pkOnlyTable = tapTable(tapField("id", "BIGINT", false, true, 1, 1));
            LinkedHashMap<String, Object> pkOnlyRow = new LinkedHashMap<>();
            pkOnlyRow.put("id", 5L);
            invoke(
                    writer,
                    "insertRowByRow",
                    new Class[]{List.class, String.class, TapTable.class, List.class},
                    List.of(pkOnlyRow),
                    "pk_only_target",
                    pkOnlyTable,
                    List.of("id")
            );
            verify(pkOnlyStatement).executeUpdate(contains("ON CONFLICT (\"id\") DO NOTHING"));
        }

        Connection nullMessageConnection = mock(Connection.class);
        Statement nullMessageStatement = mock(Statement.class);
        when(nullMessageConnection.createStatement()).thenReturn(nullMessageStatement);
        when(nullMessageStatement.executeUpdate(anyString())).thenThrow(new SQLException());
        try (ArrowWriter writer = new ArrowWriter(nullMessageConnection, false, DuckLakeConfig.disabled()).log(logger)) {
            assertThrows(SQLException.class, () -> invoke(
                    writer,
                    "insertRowByRow",
                    new Class[]{List.class, String.class, TapTable.class, List.class},
                    List.of(row(4L, "d")),
                    "null_message_target",
                    tapTableWithPk,
                    List.of("id", "name")
            ));
        }

        DuckDBAppender brokenAppender = mock(DuckDBAppender.class);
        when(brokenAppender.append((String) null)).thenThrow(new RuntimeException("append boom"));
        try (ArrowWriter writer = new ArrowWriter(mock(Connection.class), false, DuckLakeConfig.disabled()).log(logger)) {
            SQLException exception = assertThrows(SQLException.class, () -> invoke(
                    writer,
                    "appendToAppender",
                    new Class[]{DuckDBAppender.class, Object.class},
                    brokenAppender,
                    null
            ));
            assertTrue(exception.getMessage().contains("Failed to append value"));
        }
    }

    @Test
    void privateBuildersAndConvertersCoverDuckLakeSqlFormatting() throws Throwable {
        TapTable tapTable = tapTable(
                tapField("id", "BIGINT", false, true, 1, 1),
                tapField("name", "VARCHAR", true, false, null, 2)
        );
        TapTable tapTableWithoutPk = tapTable(
                tapField("id", "BIGINT", null, false, null, 1),
                tapField("name", "VARCHAR", true, false, null, 2)
        );
        TapTableDto tapTableDto = tapTableDto(
                tapFieldDto("id", "BIGINT", false, true, 1, 1),
                tapFieldDto("name", "VARCHAR", null, false, 0, 0),
                tapFieldDto("extra", "VARCHAR", true, false, null, null)
        );
        TapTableDto tapTableDtoWithoutPk = tapTableDto(
                tapFieldDto("id", "BIGINT", true, false, null, null)
        );
        tapTableDtoWithoutPk.setPrimaryKeys(Collections.emptyList());
        NodeSchemaInfo schemaInfo = nodeSchemaInfo("node_target", tapTable);
        NodeSchemaInfo schemaInfoWithoutPk = nodeSchemaInfo("node_temp", tapTableWithoutPk, Collections.emptyList());

        try (ArrowWriter localWriter = new ArrowWriter(
                mock(Connection.class),
                false,
                new DuckLakeConfig(true, "LOCAL", "/tmp/local'o", null)).log(logger);
             ArrowWriter s3Writer = new ArrowWriter(
                     mock(Connection.class),
                     false,
                     new DuckLakeConfig(true, "S3", "s3://bucket/o'clock", null)).log(logger);
             ArrowWriter disabledWriter = new ArrowWriter(mock(Connection.class), false, DuckLakeConfig.disabled()).log(logger);
             ArrowWriter otherWriter = new ArrowWriter(mock(Connection.class), false, new DuckLakeConfig(true, "OTHER", "/tmp/other", null)).log(logger);
             ArrowWriter nullConfigWriter = new ArrowWriter(mock(Connection.class), false, DuckLakeConfig.disabled()).log(logger)) {
            setField(nullConfigWriter, "duckLakeConfig", null);

            String localSql = invoke(localWriter,
                    "buildDuckLakeCreateTableSql",
                    new Class[]{String.class, TapTable.class, boolean.class},
                    "local_table", tapTable, true);
            assertTrue(localSql.contains("CREATE TEMP TABLE IF NOT EXISTS local_table"));
            assertTrue(localSql.contains("PRIMARY KEY (id)"));
            assertTrue(localSql.contains("LOCAL = '/tmp/local''o'"));

            String tapTableS3Sql = invoke(s3Writer,
                    "buildDuckLakeCreateTableSql",
                    new Class[]{String.class, TapTable.class, boolean.class},
                    "s3_table", tapTable, false);
            assertTrue(tapTableS3Sql.contains("S3 = 's3://bucket/o''clock'"));

            String tapTableOtherSql = invoke(otherWriter,
                    "buildDuckLakeCreateTableSql",
                    new Class[]{String.class, TapTable.class, boolean.class},
                    "other_table", tapTable, false);
            assertFalse(tapTableOtherSql.contains("LOCAL ="));
            assertFalse(tapTableOtherSql.contains("S3 ="));

            String tapTableNullConfigSql = invoke(nullConfigWriter,
                    "buildDuckLakeCreateTableSql",
                    new Class[]{String.class, TapTable.class, boolean.class},
                    "null_config_table", tapTable, false);
            assertFalse(tapTableNullConfigSql.contains("LOCAL ="));
            assertFalse(tapTableNullConfigSql.contains("S3 ="));

            String s3Sql = invoke(s3Writer,
                    "buildDuckLakeCreateTableSql",
                    new Class[]{String.class, TapTableDto.class, boolean.class},
                    "dto_table", tapTableDto, false);
            assertTrue(s3Sql.contains("CREATE TABLE IF NOT EXISTS dto_table"));
            assertTrue(s3Sql.contains("S3 = 's3://bucket/o''clock'"));

            String dtoLocalTempSql = invoke(localWriter,
                    "buildDuckLakeCreateTableSql",
                    new Class[]{String.class, TapTableDto.class, boolean.class},
                    "dto_temp_table", tapTableDto, true);
            assertTrue(dtoLocalTempSql.contains("CREATE TEMP TABLE IF NOT EXISTS dto_temp_table"));
            assertTrue(dtoLocalTempSql.contains("LOCAL = '/tmp/local''o'"));

            String dtoOtherSql = invoke(otherWriter,
                    "buildDuckLakeCreateTableSql",
                    new Class[]{String.class, TapTableDto.class, boolean.class},
                    "dto_other_table", tapTableDto, false);
            assertFalse(dtoOtherSql.contains("LOCAL ="));
            assertFalse(dtoOtherSql.contains("S3 ="));

            String dtoWithoutPkSql = invoke(disabledWriter,
                    "buildDuckLakeCreateTableSql",
                    new Class[]{String.class, TapTableDto.class, boolean.class},
                    "dto_without_pk", tapTableDtoWithoutPk, false);
            assertFalse(dtoWithoutPkSql.contains("PRIMARY KEY"));

            String dtoNullConfigSql = invoke(nullConfigWriter,
                    "buildDuckLakeCreateTableSql",
                    new Class[]{String.class, TapTableDto.class, boolean.class},
                    "dto_null_config", tapTableDto, false);
            assertFalse(dtoNullConfigSql.contains("LOCAL ="));
            assertFalse(dtoNullConfigSql.contains("S3 ="));

            String nodeSql = invoke(disabledWriter,
                    "buildDuckLakeCreateTableSql",
                    new Class[]{NodeSchemaInfo.class, boolean.class},
                    schemaInfo, false);
            assertTrue(nodeSql.contains("CREATE TABLE IF NOT EXISTS node_target"));
            assertFalse(nodeSql.contains("LOCAL ="));
            assertFalse(nodeSql.contains("S3 ="));

            String nodeTempLocalSql = invoke(localWriter,
                    "buildDuckLakeCreateTableSql",
                    new Class[]{NodeSchemaInfo.class, boolean.class},
                    schemaInfo, true);
            assertTrue(nodeTempLocalSql.contains("CREATE TEMP TABLE IF NOT EXISTS node_target"));
            assertTrue(nodeTempLocalSql.contains("LOCAL = '/tmp/local''o'"));

            String nodeS3Sql = invoke(s3Writer,
                    "buildDuckLakeCreateTableSql",
                    new Class[]{NodeSchemaInfo.class, boolean.class},
                    schemaInfo, false);
            assertTrue(nodeS3Sql.contains("S3 = 's3://bucket/o''clock'"));

            String nodeOtherSql = invoke(otherWriter,
                    "buildDuckLakeCreateTableSql",
                    new Class[]{NodeSchemaInfo.class, boolean.class},
                    schemaInfo, false);
            assertFalse(nodeOtherSql.contains("LOCAL ="));
            assertFalse(nodeOtherSql.contains("S3 ="));

            String nodeWithoutPkSql = invoke(disabledWriter,
                    "buildDuckLakeCreateTableSql",
                    new Class[]{NodeSchemaInfo.class, boolean.class},
                    schemaInfoWithoutPk, false);
            assertFalse(nodeWithoutPkSql.contains("PRIMARY KEY"));

            String nodeNullConfigSql = invoke(nullConfigWriter,
                    "buildDuckLakeCreateTableSql",
                    new Class[]{NodeSchemaInfo.class, boolean.class},
                    schemaInfo, false);
            assertFalse(nodeNullConfigSql.contains("LOCAL ="));
            assertFalse(nodeNullConfigSql.contains("S3 ="));

            String localWithoutPkSql = invoke(disabledWriter,
                    "buildDuckLakeCreateTableSql",
                    new Class[]{String.class, TapTable.class, boolean.class},
                    "local_without_pk", tapTableWithoutPk, false);
            assertFalse(localWithoutPkSql.contains("PRIMARY KEY"));

            String tapFieldColumn = invoke(localWriter,
                    "buildColumnDefinition",
                    new Class[]{TapField.class},
                    tapField("id", "BIGINT", false, true, 1, 1));
            assertEquals("\"id\" BIGINT NOT NULL", tapFieldColumn);

            String nullableTapFieldColumn = invoke(localWriter,
                    "buildColumnDefinition",
                    new Class[]{TapField.class},
                    tapField("nullable_name", "VARCHAR", null, false, null, 2));
            assertEquals("\"nullable_name\" VARCHAR", nullableTapFieldColumn);

            String dtoColumn = invoke(localWriter,
                    "buildColumnDefinition",
                    new Class[]{TapFieldDto.class},
                    tapFieldDto("name", "VARCHAR", true, false, 0, 0));
            assertEquals("\"name\" VARCHAR", dtoColumn);

            assertEquals("", invoke(localWriter, "escapeSqlString", new Class[]{String.class}, new Object[]{null}));
            assertEquals("a''b", invoke(localWriter, "escapeSqlString", new Class[]{String.class}, "a'b"));

            TapTable converted = invoke(localWriter,
                    "convertToTapTable",
                    new Class[]{TapTableDto.class},
                    tapTableDto);
            assertEquals("dto_id", converted.getId());
            assertEquals("dto_table", converted.getName());
            assertEquals(3, converted.getNameFieldMap().size());
            assertTrue(converted.getNameFieldMap().get("id").getPrimaryKey());
            assertEquals(1, converted.getNameFieldMap().get("id").getPrimaryKeyPos());
            assertTrue(converted.getNameFieldMap().get("name").getNullable());
            assertNull(converted.getNameFieldMap().get("extra").getPos());

            TapTable convertedWithoutFields = invoke(
                    localWriter,
                    "convertToTapTable",
                    new Class[]{TapTableDto.class},
                    new TapTableDto()
            );
            assertNull(convertedWithoutFields.getNameFieldMap());

            ArrowWriter nullableAllocatorWriter = new ArrowWriter(mock(Connection.class), false, DuckLakeConfig.disabled()).log(logger);
            setField(nullableAllocatorWriter, "allocator", null);
            nullableAllocatorWriter.close();
        }
    }

    private static long queryCount(Connection connection, String tableName) throws SQLException {
        try (Statement statement = connection.createStatement();
             java.sql.ResultSet resultSet = statement.executeQuery("SELECT count(*) FROM " + tableName)) {
            assertTrue(resultSet.next());
            return resultSet.getLong(1);
        }
    }

    private static TapTable tapTable(TapField... fields) {
        TapTable tapTable = new TapTable();
        LinkedHashMap<String, TapField> fieldMap = new LinkedHashMap<>();
        for (TapField field : fields) {
            fieldMap.put(field.getName(), field);
        }
        tapTable.setNameFieldMap(fieldMap);
        return tapTable;
    }

    private static TapField tapField(String name, String dataType, Boolean nullable, boolean primaryKey,
                                     Integer primaryKeyPos, Integer pos) {
        TapField tapField = new TapField();
        tapField.setName(name);
        tapField.setOriginalFieldName(name);
        tapField.setDataType(dataType);
        tapField.setTapType(new TapNumber());
        tapField.setNullable(nullable);
        tapField.setPrimaryKey(primaryKey);
        tapField.setPrimaryKeyPos(primaryKeyPos);
        tapField.setPos(pos);
        return tapField;
    }

    private static TapTableDto tapTableDto(TapFieldDto... fields) {
        TapTableDto tapTableDto = new TapTableDto();
        tapTableDto.setId("dto_id");
        tapTableDto.setName("dto_table");
        tapTableDto.setFields(List.of(fields));
        tapTableDto.setPrimaryKeys(List.of("id"));
        return tapTableDto;
    }

    private static TapFieldDto tapFieldDto(String name, String dataType, Boolean nullable, boolean primaryKey,
                                           Integer primaryKeyPos, Integer pos) {
        TapFieldDto dto = new TapFieldDto();
        dto.setName(name);
        dto.setOriginalFieldName(name);
        dto.setDataType(dataType);
        dto.setDuckDbTypeName(dataType);
        dto.setNullable(nullable);
        dto.setIsPrimaryKey(primaryKey);
        dto.setPrimaryKeyPos(primaryKeyPos);
        dto.setPos(pos);
        dto.setTapTypeName("TapNumber");
        return dto;
    }

    private static NodeSchemaInfo nodeSchemaInfo(String tableName, TapTable tapTable) {
        return nodeSchemaInfo(tableName, tapTable, List.of("id"));
    }

    private static NodeSchemaInfo nodeSchemaInfo(String tableName, TapTable tapTable, List<String> primaryKeys) {
        try (ArrowWriter writer = new ArrowWriter(mock(Connection.class), false, DuckLakeConfig.disabled()).log(mock(ObsLogger.class))) {
            return new NodeSchemaInfo(
                    "source_node",
                    tableName,
                    "qualified." + tableName,
                    primaryKeys,
                    tapTable.getNameFieldMap(),
                    tapTable,
                    writer.buildArrowSchema(tapTable)
            );
        }
    }

    private static Map<String, Object> row(Long id, String name) {
        LinkedHashMap<String, Object> row = new LinkedHashMap<>();
        row.put("id", id);
        row.put("name", name);
        return row;
    }

    @SuppressWarnings("unchecked")
    private static <T> T invoke(Object target, String methodName, Class<?>[] parameterTypes, Object... args) throws Throwable {
        Method method = ArrowWriter.class.getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        try {
            return (T) method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        }
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = ArrowWriter.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static class RoutingArrowWriter extends ArrowWriter {
        private boolean copyResult;
        private int copyCalls;
        private int tapTableWrites;
        private int tapTableDtoWrites;
        private int schemaInfoWrites;

        private RoutingArrowWriter(Connection connection, boolean zeroCopyEnabled, DuckLakeConfig duckLakeConfig) {
            super(connection, zeroCopyEnabled, duckLakeConfig);
        }

        @Override
        protected boolean copy(List<Map<String, Object>> data, String tableName, Schema schema) {
            copyCalls++;
            return copyResult;
        }

        @Override
        public void writeWithArrow(List<Map<String, Object>> data, String tableName, TapTable tapTable) {
            tapTableWrites++;
        }

        @Override
        public void writeWithArrow(List<Map<String, Object>> data, String tableName, TapTableDto tapTableDto) {
            tapTableDtoWrites++;
        }

        @Override
        public void writeWithArrow(List<Map<String, Object>> data, NodeSchemaInfo schemaInfo) {
            schemaInfoWrites++;
        }
    }

    private static class CopyTrackingArrowWriter extends ArrowWriter {
        private boolean copyResult;
        private int copyCalls;

        private CopyTrackingArrowWriter(Connection connection, boolean zeroCopyEnabled, DuckLakeConfig duckLakeConfig) {
            super(connection, zeroCopyEnabled, duckLakeConfig);
        }

        @Override
        protected boolean copy(List<Map<String, Object>> data, String tableName, Schema schema) {
            copyCalls++;
            return copyResult;
        }
    }

    private static class CloseFailureArrowWriter extends ArrowWriter {
        private CloseFailureArrowWriter(Connection connection, boolean zeroCopyEnabled, DuckLakeConfig duckLakeConfig) {
            super(connection, zeroCopyEnabled, duckLakeConfig);
        }

        @Override
        public VectorSchemaRoot createVectorSchemaRoot(List<Map<String, Object>> data, Schema schema) {
            VectorSchemaRoot delegate = super.createVectorSchemaRoot(data, schema);
            VectorSchemaRoot spy = org.mockito.Mockito.spy(delegate);
            org.mockito.Mockito.doAnswer(invocation -> {
                delegate.close();
                throw new RuntimeException("close failed");
            }).when(spy).close();
            return spy;
        }
    }

    private static class NullRootArrowWriter extends ArrowWriter {
        private NullRootArrowWriter(Connection connection, boolean zeroCopyEnabled, DuckLakeConfig duckLakeConfig) {
            super(connection, zeroCopyEnabled, duckLakeConfig);
        }

        @Override
        public VectorSchemaRoot createVectorSchemaRoot(List<Map<String, Object>> data, Schema schema) {
            return null;
        }
    }
}
