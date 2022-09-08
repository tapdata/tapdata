package io.tapdata.connector.dameng.cdc.logminer.util;

import io.tapdata.common.cdc.RedoLogContent;
import io.tapdata.constant.SqlConstant;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * LOB 类型工具
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/3/4 6:18 PM Create
 */
public class LobWriteUtil {
    private static final String PATTERN = ".*(select \"(.*)\" into [^ ]* from (.*) where (.*)) for update;.*";

    private final String table;
    private final String field;
    private final String where;

    public LobWriteUtil(String sqlRedo) {
        Pattern p = Pattern.compile(PATTERN);
        Matcher m = p.matcher(sqlRedo);
        if (m.find()) {
            field = m.group(2);
            table = m.group(3);
            where = m.group(4);
        } else {
            throw new RuntimeException("LOB_WRITE sqlRedo parse failed：" + sqlRedo);
        }
    }

    @Override
    public String toString() {
        return "LobWriteUtil{" +
                "table='" + table + '\'' +
                ", field='" + field + '\'' +
                ", where='" + where + '\'' +
                '}';
    }

    public static boolean isLobWrite(String operation) {
        return SqlConstant.REDO_LOG_OPERATION_LOB_TRIM.equals(operation)
                || SqlConstant.REDO_LOG_OPERATION_LOB_WRITE.equals(operation);
    }

    // 清洗 LOB_WRITE 事件，并标记反查
    // INSERT,UPDATE 事件后会紧跟 N 个 LOB_WRITE 事件；此类 INSERT,UPDATE 标记需要反查源库
    public static Map<String, List<RedoLogContent>> filterAndAddTag(Map<String, List<RedoLogContent>> redoLogContentMap) {
        RedoLogContent lastContent = null;
        List<RedoLogContent> tmpContents;
        Map<String, List<RedoLogContent>> newMap = new LinkedHashMap<>();
        for (Map.Entry<String, List<RedoLogContent>> entry : redoLogContentMap.entrySet()) {
            tmpContents = new ArrayList<>();
            for (RedoLogContent content : entry.getValue()) {
                if (LobWriteUtil.isLobWrite(content.getOperation())) {
                    LobWriteUtil lobWriteUtil = new LobWriteUtil(content.getSqlRedo());
                    if (null == lastContent || lastContent.getScn() != content.getScn()) {
                        lastContent = content;
                        lastContent.setStatus(0);
                        lastContent.setOperationCode(3);
                        lastContent.setOperation(SqlConstant.REDO_LOG_OPERATION_UPDATE);
                        lastContent.setRedoRecord(new HashMap<>());
                        tmpContents.add(lastContent);
                    }

                    Set<String> lobWriteWhere = lastContent.getLobWriteWhere();
                    if (null == lobWriteWhere) {
                        lobWriteWhere = new LinkedHashSet<>();
                        lastContent.setLobWriteWhere(lobWriteWhere);
                    }
                    lobWriteWhere.add(lobWriteUtil.where);
                } else {
                    if (StringUtils.containsAny(content.getOperation(), SqlConstant.REDO_LOG_OPERATION_INSERT, SqlConstant.REDO_LOG_OPERATION_UPDATE)) {
                        lastContent = content;
                    }
                    tmpContents.add(content);
                }
            }
            newMap.put(entry.getKey(), tmpContents);
        }
        return newMap;
    }

}
