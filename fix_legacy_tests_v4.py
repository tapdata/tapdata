#!/usr/bin/env python3
"""
增强版修复脚本：处理 FromTableConfig 和 AffectedKeyCalculator 构造器
"""

import re
import sys


def fix_from_table_config(content):
    """
    修复 FromTableConfig 构造器调用
    旧: new FromTableConfig("source", "id", "SELECT ...", List.of("id"))
    新: new FromTableConfig("source", "t")
    """

    # 模式1: 4参数 FromTableConfig (最常见)
    pattern = r'new FromTableConfig\(\s*([^,]+),\s*([^,]+),\s*[^,]+,\s*[^)]+\)'

    def replace_from_table(match):
        pre_node_id = match.group(1).strip().strip('"')
        table_alias = match.group(2).strip().strip('"')
        return f'new FromTableConfig("{pre_node_id}", "{table_alias}")'

    content = re.sub(pattern, replace_from_table, content)
    return content


def fix_affected_key_calculator_with_generator(content):
    """
    修复使用 WithCteSqlGenerator 的 AffectedKeyCalculator 调用
    旧: new AffectedKeyCalculator(..., operator, generator)
    新: new AffectedKeyCalculator(..., operator, schemaMap, querySql)
    """

    # 模式: 7参数构造器 (包含 WithCteSqlGenerator)
    pattern = r'(new AffectedKeyCalculator\()([^)]+?)(,\s*\w*generator\s*\))'

    def replace_with_generator(match):
        prefix = match.group(1)
        params = match.group(2).rstrip(',').rstrip()
        suffix = ')'
        return f'{prefix}{params},\n                createDefaultSchemaMap(),\n                "SELECT * FROM target__main"{suffix}'

    content = re.sub(pattern, replace_with_generator, content, flags=re.DOTALL | re.IGNORECASE)
    return content


def add_imports_and_helpers(content):
    """添加必要的 import 和辅助方法"""

    # 检查是否已有 NodeSchemaInfo import
    if 'import io.tapdata.flow.engine.V2.node.duckdb.NodeSchemaInfo;' not in content:
        content = content.replace(
            'import io.tapdata.flow.engine.V2.node.duckdb.FromTableConfig;',
            'import io.tapdata.flow.engine.V2.node.duckdb.FromTableConfig;\nimport io.tapdata.flow.engine.V2.node.duckdb.NodeSchemaInfo;'
        )

    # 检查是否需要添加辅助方法
    if 'createDefaultSchemaMap' not in content:
        helper = '''

    private Map<String, NodeSchemaInfo> createDefaultSchemaMap() {
        Map<String, NodeSchemaInfo> schemaMap = new HashMap<>();
        NodeSchemaInfo schema = Mockito.mock(NodeSchemaInfo.class);
        when(schema.getPrimaryKeys()).thenReturn(Collections.singletonList("id"));
        when(schema.getFieldNames()).thenReturn(List.of("id", "name"));
        when(schema.getFieldMap()).thenReturn(new HashMap<>());
        schemaMap.put("main", schema);
        return schemaMap;
    }

'''
        # 在类定义后、第一个方法前插入
        marker = '\n    @Test'
        if marker in content:
            content = content.replace(marker, helper + marker)

    return content


def main():
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else input_file

    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()

    print(f"🔧 Fixing: {input_file}")

    original_size = len(content)

    # 步骤1: 修复 FromTableConfig
    content = fix_from_table_config(content)

    # 步骤2: 修复带 generator 的 AffectedKeyCalculator
    content = fix_affected_key_calculator_with_generator(content)

    # 步骤3: 添加 imports 和辅助方法
    content = add_imports_and_helpers(content)

    print(f"📊 Size change: {original_size} -> {len(content)} chars")

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(content)

    print(f"✅ Saved: {output_file}")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python fix_enhanced.py <input_file> [output_file]")
        sys.exit(1)
    main()
