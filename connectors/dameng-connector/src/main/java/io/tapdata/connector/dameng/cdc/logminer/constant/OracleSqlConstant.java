package io.tapdata.connector.dameng.cdc.logminer.constant;

public class OracleSqlConstant {

    public static final String LAST_DICT_ARCHIVE_LOG_BY_SCN = "\n" +
            "SELECT\n" +
            "  NAME,\n" +
            "  SEQUENCE#,\n" +
            "  DICTIONARY_BEGIN d_beg,\n" +
            "  DICTIONARY_END   d_end,\n" +
            "  first_change#,\n" +
            "  next_change#\n" +
            "FROM V$ARCHIVED_LOG\n" +
            "WHERE SEQUENCE# = (SELECT MAX(SEQUENCE#)\n" +
            "                   FROM V$ARCHIVED_LOG\n" +
            "                   WHERE DICTIONARY_END = 'YES' AND next_change# > %s)";

    public static final String CHECK_CURRENT_SCN = "SELECT CKPT_LSN FROM V$rlog";


    public static final String GET_FIRST_ONLINE_REDO_LOG_FILE_FOR_10G_AND_9I = "SELECT t.*\n" +
            "FROM (\n" +
            "       SELECT\n" +
            "         l.FIRST_TIME,\n" +
            "         l.FIRST_CHANGE#,\n" +
            "         l.STATUS                status,\n" +
            "         l.sequence#,\n" +
            "       FROM v$archived_log l " +
            "       WHERE l.STATUS = 'A' and  l.deleted = 'NO'\n" +
            "       ORDER BY l.FIRST_CHANGE#) t\n" +
            "WHERE ROWNUM <= 1\n";

    public static final String GET_FIRST_ONLINE_REDO_LOG_FILE_BY_SCN_FOR_10G_AND_9I = "SELECT t.*\n" +
            "FROM (\n" +
            "       SELECT\n" +
            "         l.FIRST_TIME,\n" +
            "         l.FIRST_CHANGE#,\n" +
            "         l.STATUS                status,\n" +
            "         l.sequence#,\n" +
            "       FROM v$archived_log l \n" +
            "       WHERE l.STATUS = 'A' and  l.deleted = 'NO' AND l.FIRST_CHANGE# <= %s\n" +
            "       ORDER BY l.FIRST_CHANGE# DESC) t\n" +
            "WHERE ROWNUM <= 1\n";

    public static final String GET_FIRST_ONLINE_REDO_LOG_FILE = "SELECT t.*\n" +
            "FROM (\n" +
            "       SELECT\n" +
            "         lf.MEMBER               NAME,\n" +
            "         l.FIRST_TIME,\n" +
            "         l.FIRST_CHANGE#,\n" +
            "         l.STATUS                status,\n" +
            "         l.sequence#,\n" +
            "         l.NEXT_CHANGE#,\n" +
            "         l.NEXT_TIME\n" +
            "       FROM v$archived_log l \n" +
            "       WHERE  l.STATUS = 'A' and  l.deleted = 'NO'\n" +
            "       ORDER BY l.FIRST_CHANGE#) t\n" +
            "WHERE ROWNUM <= 1\n";

    public static final String GET_FIRST_ONLINE_REDO_LOG_FILE_BY_SCN = "SELECT t.*\n" +
            "FROM (\n" +
            "       SELECT\n" +
            "         l.FIRST_TIME,\n" +
            "         l.FIRST_CHANGE#,\n" +
            "         l.STATUS                status,\n" +
            "         l.sequence#,\n" +
            "         l.NEXT_CHANGE#,\n" +
            "         l.NEXT_TIME\n" +
            "       FROM v$archived_log l \n" +
            "       WHERE l.STATUS = 'A' and  l.deleted = 'NO'  AND l.FIRST_CHANGE# <= %s\n" +
            "       ORDER BY l.FIRST_CHANGE# DESC) t\n" +
            "WHERE ROWNUM <= 1\n";

    public static final String START_LOG_MINOR_CONTINUOUS_MINER_SQL = "  SYS.DBMS_LOGMNR.START_LOGMNR(" +
            "                               STARTSCN => %s,\n" +
            "                               OPTIONS => 2130\n" +
                     "  )\n";

    public static final String END_LOG_MINOR_SQL = "SYS.DBMS_LOGMNR.END_LOGMNR()";

    public static final String GET_REDO_LOG_RESULT_ORACLE_LOG_COLLECT_SQL = "SELECT SCN,OPERATION,TIMESTAMP,STATUS,SQL_REDO,SQL_UNDO,ROW_ID,TABLE_NAME,RS_ID,SSN," +
            "(XIDUSN || '.' || XIDSLT || '.' || XIDSQN) AS XID, OPERATION_CODE, SEG_OWNER, CSF, ROLL_BACK , THREAD#, COMMIT_TIMESTAMP, INFO FROM V$LOGMNR_CONTENTS" +
            " WHERE  SCN > %s  %s %s ";

    public static final String SWITCH_TO_CDB_ROOT = "ALTER SESSION SET CONTAINER = CDB$ROOT";

    public static final String GET_TABLE_OBJECT_ID_WITH_CLAUSE = "SELECT OBJECT_NAME, OBJECT_ID\n" +
            "FROM ALL_OBJECTS WHERE OBJECT_TYPE='TABLE' AND OWNER IN (%s) AND OBJECT_NAME IN (%s)";

    public static final String GET_INSTANCES_INFO_SQL = "SELECT  INSTANCE_NAME FROM v$instance";

    /**
     * oracle archived logs sql
     */
    public static final String ARCHIVED_LOG_SQL = "SELECT ROWNUM AS rowno,t.* FROM (SELECT * from v$archived_log WHERE (FIRST_CHANGE# >=? AND NAME IS NOT NULL and  STATUS='A' ) ORDER BY sequence# ) t WHERE rownum <= ?";


    public static final String GET_LAST_PROCESS_ARCHIVED_REDO_LOG_FILE_SQL = "SELECT t.FIRST_CHANGE# " +
            "    FROM (SELECT * " +
            "          FROM v$archived_log " +
            "          WHERE FIRST_CHANGE# <= %s AND NEXT_CHANGE# >= %s " +
            "          ORDER BY FIRST_CHANGE# DESC) t" +
            "    WHERE ROWNUM <= 1";

    public static final String GET_LAST_PROCESS_ONLINE_REDO_LOG_FILE_SQL = "SELECT\n" +
            "  t.FIRST_CHANGE#\n" +
            "FROM (\n" +
            "       SELECT\n" +
            "        l.FIRST_TIME,\n" +
            "        l.FIRST_CHANGE#,\n" +
            "        l.STATUS    status,\n" +
            "        l.sequence#\n" +
             "      FROM v$archived_log l \n" +
            "      WHERE (\n" +
            "              l.FIRST_CHANGE# < %s )\n" +
            "      ORDER BY l.FIRST_CHANGE# DESC ) t\n" +
            "WHERE ROWNUM <= 1\n";


    public static final String ADD_LOGFILE_SQL = " SYS.DBMS_LOGMNR.ADD_LOGFILE('%s')";

    public static final String START_LOG_MINOR_DIC_FROM_REDO_LOGS_SQL = " SYS.DBMS_LOGMNR.START_LOGMNR(OPTIONS=>SYS.DBMS_LOGMNR.DICT_FROM_REDO_LOGS)";
}
