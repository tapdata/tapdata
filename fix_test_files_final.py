#!/usr/bin/env python3
"""
Final comprehensive fix for FromTableConfig test files
Handles:
1. Old 4-param constructor calls
2. Missing closing parentheses
3. Extra parentheses
"""
import os
import re
import glob

def fix_test_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original = content
    
    # Fix 1: Replace old 4-param constructor with new 2-param constructor
    # Pattern: new FromTableConfig("table", "pk", "sql", fields)
    # New:     new FromTableConfig("preNodeId", "alias")
    
    # Simple case: string literals for all 4 params
    content = re.sub(
        r'new\s+FromTableConfig\(\s*"([^"]+)"\s*,\s*"([^"]+)"\s*,\s*"[^"]*"\s*,\s*\w+\)',
        r'new FromTableConfig("\1", "\2")',
        content
    )
    
    # Fix 2: Remove extra closing paren from Collections.singletonList
    # Pattern: Collections.singletonList(new FromTableConfig(...)));
    # Fix:    Collections.singletonList(new FromTableConfig(...))
    content = re.sub(
        r'(Collections\.singletonList\(new\s+FromTableConfig\([^)]+\))\)\s*\)\s*;',
        r'\1);',
        content
    )
    
    # Fix 3: Fix Arrays.asList with wrong structure
    # Pattern: Arrays.asList(new FromTableConfig(...)), new FromTableConfig(...))
    # Fix:    Arrays.asList(new FromTableConfig(...), new FromTableConfig(...))
    content = re.sub(
        r'Arrays\.asList\(\s*(new\s+FromTableConfig\([^)]+\))\s*,\s*(new\s+FromTableConfig\([^)]+)\)\s*\)',
        r'Arrays.asList(\1, \2)',
        content
    )
    
    if content != original:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"✅ Fixed: {os.path.basename(filepath)}")
        return True
    else:
        return False

if __name__ == "__main__":
    base_path = "/Users/hj/workspace/tapdata/iengine/iengine-app/src/test"
    
    fixed_count = 0
    
    for filepath in glob.glob(os.path.join(base_path, "**/*Test.java"), recursive=True):
        # Skip our new test files
        if any(x in filepath for x in ["FromTableConfigTest", "DuckDbOperatorTableManagementTest", "SqlAliasResolverTest"]):
            continue
            
        if fix_test_file(filepath):
            fixed_count += 1
    
    print(f"\n📊 Summary: {fixed_count} files fixed")
