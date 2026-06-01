#!/usr/bin/env python3
"""
Batch fix FromTableConfig constructor calls in test files
Old: new FromTableConfig("table", "pk"))
New: new FromTableConfig("node_id", "alias")
"""
import os
import re
import glob

def fix_test_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original = content
    
    # Pattern 1: Fix constructor calls with extra closing paren
    # new FromTableConfig("table_name", "pk_field")) -> new FromTableConfig("preNodeId", "tableNameInSql")
    content = re.sub(
        r'new\s+FromTableConfig\(\s*"([^"]+)"\s*,\s*"([^"]+)"\s*\)\)',
        r'new FromTableConfig("\1", "\2")',
        content
    )
    
    # Pattern 2: Fix Arrays.asList with wrong structure
    # Arrays.asList(new FromTableConfig(...)), new FromTableConfig(...)) 
    # -> Arrays.asList(new FromTableConfig(...), new FromTableConfig(...))
    content = re.sub(
        r'Arrays\.asList\((new\s+FromTableConfig\([^)]+\))\),\s*(new\s+FromTableConfig\([^)]+\))\)',
        r'Arrays.asList(\1, \2)',
        content
    )
    
    if content != original:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"✅ Fixed: {os.path.basename(filepath)}")
        return True
    else:
        print(f"⏭️  No changes: {os.path.basename(filepath)}")
        return False

if __name__ == "__main__":
    base_path = "/Users/hj/workspace/tapdata/iengine/iengine-app/src/test"
    
    patterns = [
        "**/*ScenariosTest.java",
        "**/*IntegrationTest.java"
    ]
    
    fixed_count = 0
    total_count = 0
    
    for pattern in patterns:
        for filepath in glob.glob(os.path.join(base_path, pattern), recursive=True):
            total_count += 1
            if fix_test_file(filepath):
                fixed_count += 1
    
    print(f"\n📊 Summary: {fixed_count}/{total_count} files fixed")
