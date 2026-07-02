package io.tapdata.flow.engine.V2.common;

import com.tapdata.entity.TapdataCompleteTableSnapshotEvent;
import com.tapdata.entity.TapdataEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@DisplayName("Class StreamReadTag Test")
class StreamReadTagTest {

    @Nested
    @DisplayName("Constructor test")
    class ConstructorTest {
        @Test
        @DisplayName("should keep constructor arguments")
        void shouldKeepConstructorArguments() {
            Function<List<String>, List<String>> targetTableName = mock(Function.class);
            Consumer<List<String>> startStreamRead = mock(Consumer.class);

            StreamReadTag streamReadTag = new StreamReadTag(targetTableName, startStreamRead);

            assertSame(targetTableName, streamReadTag.targetTableName);
            assertSame(startStreamRead, streamReadTag.startStreamRead);
        }
    }

    @Nested
    @DisplayName("Method accept event test")
    class AcceptEventTest {
        @Test
        @DisplayName("should return directly when startStreamRead is null")
        void shouldReturnDirectlyWhenStartStreamReadIsNull() {
            Function<List<String>, List<String>> targetTableName = mock(Function.class);
            StreamReadTag streamReadTag = new StreamReadTag(targetTableName, null);

            assertDoesNotThrow(() -> streamReadTag.accept(new TapdataCompleteTableSnapshotEvent("test_table")));
            verifyNoInteractions(targetTableName);
        }

        @Test
        @DisplayName("should ignore non snapshot event")
        void shouldIgnoreNonSnapshotEvent() {
            Function<List<String>, List<String>> targetTableName = mock(Function.class);
            Consumer<List<String>> startStreamRead = mock(Consumer.class);
            StreamReadTag streamReadTag = new StreamReadTag(targetTableName, startStreamRead);

            streamReadTag.accept(new TapdataEvent());

            verifyNoInteractions(targetTableName, startStreamRead);
        }

        @Test
        @DisplayName("should transform table name and start stream read for snapshot event")
        void shouldTransformTableNameAndStartStreamReadForSnapshotEvent() {
            Function<List<String>, List<String>> targetTableName = mock(Function.class);
            Consumer<List<String>> startStreamRead = mock(Consumer.class);
            StreamReadTag streamReadTag = new StreamReadTag(targetTableName, startStreamRead);
            List<String> sourceTableNames = List.of("source_table");
            List<String> targetTableNames = List.of("target_table");
            when(targetTableName.apply(sourceTableNames)).thenReturn(targetTableNames);

            streamReadTag.accept(new TapdataCompleteTableSnapshotEvent("source_table"));

            verify(targetTableName).apply(sourceTableNames);
            verify(startStreamRead).accept(targetTableNames);
        }
    }

    @Nested
    @DisplayName("Method accept table names test")
    class AcceptTableNamesTest {
        @Test
        @DisplayName("should return directly when startStreamRead is null")
        void shouldReturnDirectlyWhenStartStreamReadIsNull() {
            StreamReadTag streamReadTag = new StreamReadTag(mock(Function.class), null);

            assertDoesNotThrow(() -> streamReadTag.accept(List.of("table1")));
        }

        @Test
        @DisplayName("should pass through table names")
        void shouldPassThroughTableNames() {
            Consumer<List<String>> startStreamRead = mock(Consumer.class);
            StreamReadTag streamReadTag = new StreamReadTag(mock(Function.class), startStreamRead);
            List<String> tableNames = List.of("table1", "table2");

            streamReadTag.accept(tableNames);

            verify(startStreamRead).accept(tableNames);
        }
    }
}
