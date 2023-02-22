package io.tapdata.pdk.tdd.tests.v2;

import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.tests.support.TapGo;
import io.tapdata.pdk.tdd.tests.support.TapTestCase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.tapdata.entity.simplify.TapSimplify.list;

@DisplayName("discoverSchema.test")//discoverSchema发现表， 必测方法
@TapGo(tag = "V2", sort = 9800, goTest = true, isSub = true)//6
public class DiscoverSchemaTestV2 extends DiscoverSchemaTest {
    @DisplayName("discoverSchema.discover")//用例1， 发现表
    @Test
    @TapTestCase(sort = 1)
    /**
     * 执行discoverSchema之后， 至少返回一张表， 表里有表名即为成功
     * 表里没有字段描述时， 报警告
     * 表里有字段， 但是字段的name或者dataType为空时， 报警告， 具体哪些字段有问题
     * */
    void discover() {
        super.discover();
    }


    @DisplayName("discoverSchema.discoverByTableName1")//用例3， 通过指定表明加载特定表（依赖已经存在多表）
    @Test
    @TapTestCase(sort = 2)
    /**
     * 执行discoverSchema之后，
     * 发现有大于1张表的返回，
     * 通过指定第一张表之后的任意一张表名，
     * 通过List<String> tables参数指定那张表，
     * 通过Consumer<List<TapTable>> consumer返回了这一张且仅此一张表为成功。
     * 如果只有一张表， 直接通过此测试。
     * */
    void discoverByTableName1() {
        super.discoverByTableName1();
    }

    @DisplayName("discoverSchema.discoverByTableCount1")//用例5， 通过指定表数量加载固定数量的表（依赖已经存在多表）
    @Test
    @TapTestCase(sort = 3)
    /**
     * 执行discoverSchema之后，
     * 发现有大于1张表的返回，
     * 通过int tableSize参数指定为1，
     * 通过Consumer<List<TapTable>> consumer返回了一张表为成功。
     * 如果只有一张表， 直接通过此测试。
     * */
    void discoverByTableCount1() {
        super.discoverByTableCount1();
    }

    public static List<SupportFunction> testFunctions() {
        return list();
    }
}
