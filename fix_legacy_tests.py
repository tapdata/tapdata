#!/usr/bin/env python3
"""
批量修复 AffectedKeyCalculator 构造器调用
将旧的 6/7 参数构造器替换为新的 8 参数构造器
"""

import re
import sys

def fix_constructor_calls(content):
    """
    替换所有 AffectedKeyCalculator 构造器调用
    """

    # 模式1: 6参数构造器 (最常见)
    # new AffectedKeyCalculator(
    #     param1,
    #     param2,
    #     param3,
    #     param4,
    #     param5,
    #     param6
    # )
    pattern6 = r'(new AffectedKeyCalculator\(\s*)([^)]+?)(\)\s*)'

    def replace_6_params(match):
        prefix = match.group(1)
        params = match.group(2)
        suffix = match.group(3)

        # 清理参数，提取各个参数值
        lines = [line.strip().rstrip(',') for line in params.split('\n') if line.strip()]
        if len(lines) >= 6:
            # 提取前6个参数
            params_clean = ',\n                '.join(lines[:6])
            return (f'{prefix}{params_clean},\n'
                    f'                createDefaultSchemaMap(),\n'
                    f'                "SELECT * FROM target__users"\n'
                    f'{suffix}')
        return match.group(0)  # 不匹配则返回原样

    result = re.sub(pattern6, replace_6_params, content, flags=re.DOTALL)

    return result


def add_helper_method(content):
    """添加 createDefaultSchemaMap 辅助方法"""

    helper_method = '''
    // ==================== Schema 工厂 ====================

    private Map<String, NodeSchemaInfo> createDefaultSchemaMap() {
        Map<String, NodeSchemaInfo> schemaMap = new HashMap<>();

        NodeSchemaInfo userSchema = Mockito.mock(NodeSchemaInfo.class);
        when(userSchema.getPrimaryKeys()).thenReturn(Collections.singletonList("id"));
        when(userSchema.getFieldNames()).thenReturn(Arrays.asList("id", "name", "email"));
        when(userSchema.getFieldMap()).thenReturn(new HashMap<>());
        schemaMap.put("node_users", userSchema);

        return schemaMap;
    }

'''

    # 在 @BeforeEach 方法之前插入
    marker = '    @BeforeEach\n    void setUp()'
    if marker in content:
        content = content.replace(marker, helper_method + marker)

    return content


def main():
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else input_file

    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()

    print(f"📖 Reading: {input_file}")
    print(f"📊 File size: {len(content)} chars")

    # 统计原始构造器调用次数
    original_count = len(re.findall(r'new AffectedKeyCalculator\(', content))
    print(f"🔍 Found {original_count} constructor calls")

    # 步骤1: 添加辅助方法
    content = add_helper_method(content)

    # 步骤2: 替换构造器调用
    content = fix_constructor_calls(content)

    # 统计修复后构造器调用次数（应该都是8参数）
    fixed_count = len(re.findall(r'new AffectedKeyCalculator\(', content))
    print(f"✅ Fixed to {fixed_count} constructor calls (8-param version)")

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(content)

    print(f"💾 Written to: {output_file}")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python fix_constructor.py <input_file> [output_file]")
        sys.exit(1)
    main()
