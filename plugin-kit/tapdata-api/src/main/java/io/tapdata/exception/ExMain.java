package io.tapdata.exception;

import io.tapdata.exception.pdk.TapPDKBatchReadMissingPrivilegesException;

import java.sql.SQLException;
import java.util.Collections;

/**
 * @author samuel
 * @Description
 * @create 2023-02-23 11:19
 **/
public class ExMain {
	public static void main(String[] args) {
		/*try {
			doInitial();
		} catch (Exception e) {
			if (e instanceof TapCodeException) {
				System.out.println(((TapCodeException) e).stackTrace2String());
			}
			e.printStackTrace();
		}*/

		try {
			test3();
		} catch (Exception e) {
			if (e instanceof TapCodeException) {
				System.out.println(((TapCodeException) e).stackTrace2String());
			}
			e.printStackTrace();
		}
	}

	private static void executeSql() throws SQLException {
		throw new SQLException("Missing select privileges", "SQL001", 0);
	}

	private static void batchRead() {
		try {
			executeSql();
		} catch (SQLException e) {
			throw new RuntimeException("Batch read failed", e);
		}
	}

	private static void executeMethod() {
		try {
			batchRead();
		} catch (Exception e) {
			throw new TapPDKBatchReadMissingPrivilegesException("Oracle", "select * from table", Collections.singletonList("SELECT TABLE"), e);
		}
	}

	private static void doInitial() {
		try {
			executeMethod();
		} catch (Exception e) {
			throw new TapCodeException(SourcePdkEx_11.BATCH_READ_FROM_PDK, "Call pdk batch read method failed", e);
		}
	}

	private static void test1() {
		throw new NullPointerException();
	}

	private static void test2() {
		try {
			test1();
		} catch (Exception e) {
			throw new RuntimeException("test2 failed", e);
		}
	}

	private static void test3() {
		try {
			test2();
		} catch (Exception e) {
			throw new TapCodeException(SourcePdkEx_11.DOWNLOAD_PDK_FAILED, "test3 failed", e);
		}
	}
}
