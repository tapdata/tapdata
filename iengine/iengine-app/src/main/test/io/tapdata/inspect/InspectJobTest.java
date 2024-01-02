package io.tapdata.inspect;


import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class InspectJobTest {
    Clob clob;
    Clob errorClob;
    @Before
    public void before(){
        clob = new Clob() {
            @Override
            public long length() throws SQLException {
                return 0;
            }

            @Override
            public String getSubString(long pos, int length) throws SQLException {
                return "Clob";
            }

            @Override
            public Reader getCharacterStream() throws SQLException {
                return null;
            }

            @Override
            public InputStream getAsciiStream() throws SQLException {
                return null;
            }

            @Override
            public long position(String searchstr, long start) throws SQLException {
                return 0;
            }

            @Override
            public long position(Clob searchstr, long start) throws SQLException {
                return 0;
            }

            @Override
            public int setString(long pos, String str) throws SQLException {
                return 0;
            }

            @Override
            public int setString(long pos, String str, int offset, int len) throws SQLException {
                return 0;
            }

            @Override
            public OutputStream setAsciiStream(long pos) throws SQLException {
                return null;
            }

            @Override
            public Writer setCharacterStream(long pos) throws SQLException {
                return null;
            }

            @Override
            public void truncate(long len) throws SQLException {

            }

            @Override
            public void free() throws SQLException {

            }

            @Override
            public Reader getCharacterStream(long pos, long length) throws SQLException {
                return null;
            }
        };

        errorClob = new Clob() {
            @Override
            public long length() throws SQLException {
                return 0;
            }

            @Override
            public String getSubString(long pos, int length) throws SQLException {
                throw new SQLException();
            }

            @Override
            public Reader getCharacterStream() throws SQLException {
                return null;
            }

            @Override
            public InputStream getAsciiStream() throws SQLException {
                return null;
            }

            @Override
            public long position(String searchstr, long start) throws SQLException {
                return 0;
            }

            @Override
            public long position(Clob searchstr, long start) throws SQLException {
                return 0;
            }

            @Override
            public int setString(long pos, String str) throws SQLException {
                return 0;
            }

            @Override
            public int setString(long pos, String str, int offset, int len) throws SQLException {
                return 0;
            }

            @Override
            public OutputStream setAsciiStream(long pos) throws SQLException {
                return null;
            }

            @Override
            public Writer setCharacterStream(long pos) throws SQLException {
                return null;
            }

            @Override
            public void truncate(long len) throws SQLException {

            }

            @Override
            public void free() throws SQLException {

            }

            @Override
            public Reader getCharacterStream(long pos, long length) throws SQLException {
                return null;
            }
        };
    }

    @Test
    public void testDiffRecordTypeConvert() {
        final Map<String, Object> inputRecord = new HashMap<>();
        inputRecord.put("clob",clob);
        final Map<String, Object> result = InspectJob.diffRecordTypeConvert(inputRecord, Arrays.asList("clob"));
        assertEquals("Clob",result.get("clob"));
    }

    @Test
    public void testDiffRecordTypeConvert_errorCLob() {
        final Map<String, Object> inputRecord = new HashMap<>();
        inputRecord.put("clob",errorClob);
        final Map<String, Object> result = InspectJob.diffRecordTypeConvert(inputRecord, Arrays.asList("clob"));
        assertNull(result.get("clob"));
    }
}
