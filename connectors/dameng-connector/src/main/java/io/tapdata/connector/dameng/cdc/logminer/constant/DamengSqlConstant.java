package io.tapdata.connector.dameng.cdc.logminer.constant;

public class DamengSqlConstant {


    public static final String CHECK_CURRENT_SCN = "SELECT CKPT_LSN FROM V$rlog";


    public static final String START_LOG_MINOR_CONTINUOUS_MINER_SQL = "  SYS.DBMS_LOGMNR.START_LOGMNR(" +
            "                               STARTSCN => %s,\n" +
            "                               OPTIONS => 2130\n" +
                     "  )\n";

    public static final String END_LOG_MINOR_SQL = "SYS.DBMS_LOGMNR.END_LOGMNR()";

    public static final String GET_REDO_LOG_RESULT_ORACLE_LOG_COLLECT_SQL = "SELECT SCN,OPERATION,TIMESTAMP,STATUS,SQL_REDO,SQL_UNDO,ROW_ID,TABLE_NAME,RS_ID,SSN," +
            " XID, OPERATION_CODE, SEG_OWNER, CSF, ROLL_BACK , THREAD#, COMMIT_TIMESTAMP, INFO FROM V$LOGMNR_CONTENTS" +
            " WHERE  operation != 'SELECT_FOR_UPDATE' AND SCN > %s AND (operation = 'COMMIT' OR (operation = 'ROLLBACK' AND PXID != '0000000000000000') OR (%s %s)) ";

    public static final String SWITCH_TO_CDB_ROOT = "ALTER SESSION SET CONTAINER = CDB$ROOT";

    public static final String GET_TABLE_OBJECT_ID_WITH_CLAUSE = "SELECT OBJECT_NAME, OBJECT_ID\n" +
            "FROM ALL_OBJECTS WHERE OBJECT_TYPE='TABLE' AND OWNER IN (%s) AND OBJECT_NAME IN (%s)";

    public static final String GET_INSTANCES_INFO_SQL = "SELECT  INSTANCE_NAME FROM v$instance";

    /**
     * oracle archived logs sql
     */
    public static final String ARCHIVED_LOG_SQL = "SELECT ROWNUM AS rowno,t.* FROM (SELECT * from v$archived_log WHERE (FIRST_CHANGE# >=? and next_CHANGE#>? AND NAME IS NOT NULL and  STATUS='A' ) ORDER BY sequence# ) t WHERE rownum <= ?";


    public static final String GET_LAST_PROCESS_ARCHIVED_REDO_LOG_FILE_SQL = "SELECT t.FIRST_CHANGE# " +
            "    FROM (SELECT * " +
            "          FROM v$archived_log " +
            "          WHERE FIRST_CHANGE# <= %s AND NEXT_CHANGE# >= %s " +
            "          ORDER BY FIRST_CHANGE# DESC) t" +
            "    WHERE ROWNUM <= 1";



}
