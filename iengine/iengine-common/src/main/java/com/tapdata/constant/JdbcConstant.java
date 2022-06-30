package com.tapdata.constant;

public class JdbcConstant {

	/**
	 * oracle offset sql
	 */
	public final static String ORACLE_OFFSET_TABLE_NAME = "_TAPD8_OFFSET";
	public final static String ORACLE_CREATE_OFFSET_TABLE = "CREATE TABLE %s(" +
			"JOB_ID VARCHAR2(50), " +
			"SCN NUMBER NOT NULL, " +
			"RSID VARCHAR2(50) NOT NULL)";
	public final static String POSTGRES_ORACLE_CREATE_OFFSET_TABLE = "CREATE TABLE %s(" +
			"JOB_ID VARCHAR(50), " +
			"SCN NUMERIC NOT NULL, " +
			"RSID VARCHAR(50) NOT NULL)";
	public final static String ORACLE_CHECK_OFFSET_EXISTS = "SELECT COUNT(1) FROM %s WHERE JOB_ID=? AND SCN=? AND RSID=?";
	public final static String ORACLE_INSERT_OFFSET = "INSERT INTO %s(JOB_ID, SCN, RSID) VALUES(?, ?, ?)";
	public final static String ORACLE_UPDATE_SCN_RSID = "UPDATE %s SET SCN=?, RSID=? WHERE JOB_ID=?";
	public final static String ORACLE_SELECT_OFFSET_IS_MAX = "SELECT COUNT(1) FROM %s WHERE JOB_ID=? AND SCN>?";
	public final static String ORACLE_DELETE_OFFSET_BY_JOBID = "DELETE FROM %s WHERE JOB_ID=?";
	public final static String ORACLE_DELETE_OFFSET_BY_JOBID_SCN = "DELETE FROM %s WHERE JOB_ID=? AND SCN<?";
	public final static String ORACLE_SELECT_MAX_OFFSET_SCN_BY_JOBID = "SELECT MAX(SCN) FROM %s WHERE JOB_ID=?";
	public final static String ORACLE_SELECT_OFFSET_IS_PROCESSED = "SELECT COUNT(1) FROM %s WHERE JOB_ID=? AND (SCN>? OR (SCN=? AND RSID=?))";
	public final static String ORACLE_SELECT_GROUP_BY_JOBID = "SELECT JOB_ID FROM %s GROUP BY JOB_ID";
}
