package io.tapdata.inspect.compare;

import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class DefaultCompareTest {

    private DefaultCompare defaultCompareUnderTest;

    Clob clob;

    Clob errorClob;

    @Before
    public void setUp() {
        defaultCompareUnderTest = new DefaultCompare(Arrays.asList("value"), Arrays.asList("value"));
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
    public void testTry2String() {
        assertEquals("Clob", defaultCompareUnderTest.try2String(clob));
    }

    @Test
    public void testTry2String_error() {
       assertEquals(errorClob,defaultCompareUnderTest.try2String(errorClob));
    }

}
