package io.tapdata.connector.mysql.util;

import com.mysql.cj.jdbc.exceptions.MysqlDataTruncation;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.exception.TapPdkBaseException;
import io.tapdata.exception.TapPdkViolateNullableEx;
import io.tapdata.exception.TapPdkViolateUniqueEx;
import io.tapdata.exception.TapPdkWriteLengthEx;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.sql.SQLIntegrityConstraintViolationException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/4/13 15:10 Create
 */
public class ExceptionWrapper {

    public static RuntimeException wrap(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent, Throwable e, Function<TapPdkBaseException, RuntimeException> fn) {
        TapPdkBaseException newEx = null;
        if (e instanceof TapPdkBaseException) {
            newEx = (TapPdkBaseException) e;
        } else if (e instanceof SQLIntegrityConstraintViolationException) {
            SQLIntegrityConstraintViolationException ins = (SQLIntegrityConstraintViolationException) e;
            if (1048 == (ins).getErrorCode() && "23000".equals((ins).getSQLState())) {
                // [code:1048, state: 23000] java.sql.SQLIntegrityConstraintViolationException: Column 'notnull' cannot be null
                newEx = Optional.ofNullable(e.getMessage()).map(err -> {
                    Pattern p = Pattern.compile(".*Column '([^']+)' cannot be null.*");
                    Matcher m = p.matcher(err);
                    if (m.find() && m.groupCount() > 0) {
                        return m.group(1);
                    }
                    return null;
                }).map(fieldName -> tapTable.getNameFieldMap().get(fieldName)
                ).map(field -> new TapPdkViolateNullableEx(
                        tapConnectorContext.getSpecification().getId(),
                        field.getName(),
                        e
                )).orElse(null);
            } else if (1062 == (ins).getErrorCode() && "23000".equals((ins).getSQLState())) {
                // [code:1062, state: 23000] java.sql.SQLIntegrityConstraintViolationException: Duplicate entry 'ok' for key 'test_error_code.notnull'
                newEx = Optional.ofNullable(e.getMessage()).map(err -> {
                    Pattern p = Pattern.compile(".*Duplicate entry 'ok' for key '[^.]+.([^']+)'.*");
                    Matcher m = p.matcher(err);
                    if (m.find() && m.groupCount() > 0) {
                        return m.group(1);
                    }
                    return null;
                }).map(fieldName -> tapTable.getNameFieldMap().get(fieldName)).map(field -> {
                    Map<String, Object> data = tapRecordEvent.getFilter(Collections.singletonList(field.getName()));
                    if (null == data || data.isEmpty()) return null;
                    return new TapPdkViolateUniqueEx(
                            tapConnectorContext.getSpecification().getId(),
                            field.getName(),
                            data.get(field.getName()),
                            null,
                            e
                    );
                }).orElse(null);
            }
        } else if (e instanceof MysqlDataTruncation) {
            // [code:1406, state: 22001] com.mysql.cj.jdbc.exceptions.MysqlDataTruncation: Data truncation: Data too long for column 'maxlen' at row 1
            if (1406 == ((MysqlDataTruncation) e).getErrorCode() && "22001".equals(((MysqlDataTruncation) e).getSQLState())) {
                newEx = Optional.ofNullable(e.getMessage()).map(err -> {
                    Pattern p = Pattern.compile(".*Data too long for column '([^']+)' at row.*");
                    Matcher m = p.matcher(err);
                    if (m.find() && m.groupCount() > 0) {
                        return m.group(1);
                    }
                    return null;
                }).map(fieldName -> tapTable.getNameFieldMap().get(fieldName)).map(field -> {
                    Map<String, Object> data = tapRecordEvent.getFilter(Collections.singletonList(field.getName()));
                    if (null == data || data.isEmpty()) return null;
                    return new TapPdkWriteLengthEx(
                            tapConnectorContext.getSpecification().getId(),
                            field.getName(),
                            field.getDataType(),
                            data.get(field.getName()),
                            e
                    );
                }).orElse(null);
            }
        }

        return fn.apply(newEx);
    }
}
