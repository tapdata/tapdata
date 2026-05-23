/*
 *  Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.tapdata.flow.engine.V2.node.duckdb;

import org.apache.arrow.c.Data;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test inserting an Apache Arrow stream into DuckDB and verify the SQL query result.
 */
class DuckDBArrowInsertTest {

    @Test
    void testInsertArrowStreamIntoDuckDB() throws Exception {
        System.out.println("🧪 DuckDB Arrow insert test...");

        // Build a simple Arrow schema: single BIGINT column named `id`
        Schema schema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null)
        ));

        byte[] arrowBytes = createArrowBytes(schema);

        // Open an in-memory DuckDB connection
        try (DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:")) {
            try (Statement stmt = conn.createStatement()) {
                boolean duckLakeLoaded = tryEnableDuckLake(stmt);
                System.out.println("DuckLake enabled: " + duckLakeLoaded);
                stmt.execute("CREATE TABLE t(id BIGINT)");
            }

            try (RootAllocator allocator = new RootAllocator();
                 InputStream in = new ByteArrayInputStream(arrowBytes);
                 ArrowStreamReader reader = new ArrowStreamReader(in, allocator);
                 ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {

                Data.exportArrayStream(allocator, reader, stream);
                conn.registerArrowStream("test_stream", stream);

                // Create table, insert from the stream, then query to verify
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("DELETE FROM t");

                    List<Map<String, Object>> changelog = new ArrayList<>();
                    try (ResultSet rs = executeInsertReturning(stmt)) {
                        while (rs.next()) {
                            long id = rs.getLong("id");
                            Map<String, Object> change = new LinkedHashMap<>();
                            change.put("op", "INSERT");
                            change.put("table", "t");
                            change.put("id", id);
                            changelog.add(change);
                            System.out.println("CDC Changelog => " + change);
                        }
                    }

                    assertEquals(1, changelog.size(), "Expected exactly one CDC changelog entry");
                    assertEquals(100L, ((Number) changelog.get(0).get("id")).longValue());

                    try (ResultSet rs = stmt.executeQuery("SELECT id FROM t")) {
                        int rowCount = 0;
                        while (rs.next()) {
                            long id = rs.getLong(1);
                            System.out.println("  row id=" + id);
                            assertEquals(100L, id);
                            rowCount++;
                        }
                        assertEquals(1, rowCount, "Expected exactly one row inserted from Arrow stream");
                    }
                }
            }
        }
    }

    private boolean tryEnableDuckLake(Statement stmt) {
        try {
            stmt.execute("INSTALL ducklake");
        } catch (Exception e) {
            System.out.println("DuckLake install skipped: " + e.getMessage());
        }

        try {
            stmt.execute("LOAD ducklake");
            try (ResultSet rs = stmt.executeQuery("SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'ducklake'")) {
                if (rs.next()) {
                    return rs.getBoolean(1);
                }
            }
            return true;
        } catch (Exception e) {
            System.out.println("DuckLake load skipped: " + e.getMessage());
            return false;
        }
    }

    private ResultSet executeInsertReturning(Statement stmt) throws Exception {
        boolean hasResultSet = stmt.execute("INSERT INTO t SELECT * FROM test_stream RETURNING id");
        assertTrue(hasResultSet, "INSERT ... RETURNING should produce a result set for CDC changelog capture");
        return stmt.getResultSet();
    }

    private byte[] createArrowBytes(Schema schema) throws Exception {
        try (RootAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
             ByteArrayOutputStream out = new ByteArrayOutputStream();
             ArrowStreamWriter writer = new ArrowStreamWriter(root, new DictionaryProvider.MapDictionaryProvider(), out)) {

            BigIntVector idVector = (BigIntVector) root.getVector("id");
            idVector.setSafe(0, 100L);
            root.setRowCount(1);

            writer.start();
            writer.writeBatch();
            writer.end();
            return out.toByteArray();
        }
    }
}