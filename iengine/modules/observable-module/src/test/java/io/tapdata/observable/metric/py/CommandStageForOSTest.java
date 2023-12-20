package io.tapdata.observable.metric.py;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class CommandStageForOSTest {
    String os;
    @BeforeEach
    void init() {
        os = System.getProperty("os.name");
    }

    @AfterEach
    void close() {
        System.setProperty("os.name", os);
    }

    @Nested
    class GetProcessBuilderTest {
        void verify(String os) {
            System.setProperty("os.name", os);
            try(MockedStatic<CommandStageForOS> mockedStatic = mockStatic(CommandStageForOS.class)) {
                mockedStatic.when(() -> CommandStageForOS.getProcessBuilder(anyString(), anyString())).thenCallRealMethod();
                Assertions.assertDoesNotThrow(()-> CommandStageForOS.getProcessBuilder("str", "path"));
            }
        }

        @Test
        void verifyWin11OS() {
            verify("Windows 11");
        }
        @Test
        void verifyWin10OS() {
            verify("windows 10");
        }
        @Test
        void verifyMacOS() {
            verify("Mac M1");
        }
        @Test
        void verifyLinuxOS() {
            verify("ReadHat");
        }
        @Test
        void verifyUbuntuOS() {
            verify("Ubuntu 3.11.1");
        }
    }

    @Nested
    class Win10CommandTest {
        @Test
        void verifyParams () {
            Assertions.assertEquals("cd %s; java -jar %s setup.py install", Win10Command.PACKAGE_COMPILATION_COMMAND);
        }

        @Nested
        class ExecuteTest {
            @Test
            void testExecute() {
                Win10Command command = mock(Win10Command.class);
                when(command.execute(anyString(), anyString())).thenCallRealMethod();
                Assertions.assertNotNull(command.execute("aaa", "aaa"));
            }
        }
    }

    @Nested
    class LinuxShellTest {
        @Test
        void verifyParams () {
            Assertions.assertEquals("cd %s; java -jar %s setup.py install", LinuxShell.PACKAGE_COMPILATION_COMMAND);
        }

        @Nested
        class ExecuteTest {
            @Test
            void testExecute() {
                LinuxShell command = mock(LinuxShell.class);
                when(command.execute(anyString(), anyString())).thenCallRealMethod();
                Assertions.assertNotNull(command.execute("aaa", "aaa"));
            }
        }
    }
}
