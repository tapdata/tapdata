package com.tapdata.tm.taskinspect.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class IConfigTest {

    static class SampleInstantiationException implements IConfig<SampleInstantiationException> {
        public SampleInstantiationException() throws InstantiationException {
            throw new InstantiationException();
        }

        @Override
        public SampleInstantiationException init(int depth) {
            return this;
        }
    }

    static class SampleIllegalAccessException implements IConfig<SampleIllegalAccessException> {
        public SampleIllegalAccessException() throws IllegalAccessException {
            throw new IllegalAccessException();
        }

        @Override
        public SampleIllegalAccessException init(int depth) {
            return this;
        }
    }

    static class SampleConfig implements IConfig<SampleConfig> {
        @Override
        public SampleConfig init(int depth) {
            return this;
        }
    }

    SampleConfig config;

    @Mock
    SampleConfig mockConfig;

    @BeforeEach
    void setUp() {
        config = new SampleConfig();
        reset(mockConfig);
    }

    @Nested
    class initTest {

        @Nested
        class init_V_V_Test {

            @Test
            void testInit_InsNotNull() {
                // 逻辑预设：ins 不为 null
                Object ins = new Object();
                Object def = new Object();

                // 操作：调用 init 方法
                Object result = config.init(ins, def);

                // 期望：返回 ins
                assertSame(ins, result);
            }

            @Test
            void testInit_InsNull() {
                // 逻辑预设：ins 为 null
                Object ins = null;
                Object def = new Object();

                // 操作：调用 init 方法
                Object result = config.init(ins, def);

                // 期望：返回 def
                assertSame(def, result);
            }
        }

        @Nested
        class init_V_int_Class_Test {

            @Test
            void testInit_InsNotNull_DepthNegative() {
                // 逻辑预设：ins 不为 null，depth 为负数
                int depth = -1;
                Class<SampleConfig> clz = SampleConfig.class;

                // 操作：调用 init 方法
                SampleConfig result = config.init(mockConfig, depth, clz);

                // 期望：返回 ins，并调用 ins.init(depth)
                assertSame(mockConfig, result);
                verify(mockConfig).init(depth);
            }

            @Test
            void testInit_InsNotNull_DepthPositive() {
                // 逻辑预设：ins 不为 null，depth 为正数
                int depth = 2;
                Class<SampleConfig> clz = SampleConfig.class;

                // 操作：调用 init 方法
                SampleConfig result = config.init(mockConfig, depth, clz);

                // 期望：返回 ins，并调用 ins.init(depth - 1)
                assertSame(mockConfig, result);
                verify(mockConfig).init(depth - 1);
            }

            @Test
            void testInit_InsNull() {
                // 逻辑预设：ins 为 null，depth 为负数
                int depth = -1;

                // 操作：调用 init 方法
                SampleConfig result = config.init(null, depth, SampleConfig.class);

                // 期望：返回新创建的实例
                assertNotNull(result);
            }

            @Test
            void testInit_InsNull_InstantiationException() {
                // 逻辑预设：ins 为 null，depth 为正数，但实例化失败
                SampleInstantiationException ins = null;
                int depth = 2;
                Class<SampleInstantiationException> clz = SampleInstantiationException.class;

                // 操作：调用 init 方法
                Exception thrown = assertThrows(RuntimeException.class, () -> config.init(ins, depth, clz));

                // 期望：抛出 RuntimeException
                assertNotNull(thrown.getCause());
                assertInstanceOf(InstantiationException.class, thrown.getCause());

            }

            @Test
            void testInit_InsNull_IllegalAccessException() {
                // 逻辑预设：ins 为 null，depth 为正数，但实例化失败
                SampleIllegalAccessException ins = null;
                int depth = 2;
                Class<SampleIllegalAccessException> clz = SampleIllegalAccessException.class;

                // 操作：调用 init 方法
                Exception thrown = assertThrows(RuntimeException.class, () -> config.init(ins, depth, clz));

                // 期望：抛出 RuntimeException
                assertNotNull(thrown.getCause());
                assertTrue(thrown.getCause() instanceof IllegalAccessException);
            }
        }
    }
}
