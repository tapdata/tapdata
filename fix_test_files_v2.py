#!/usr/bin/env python3
"""
Fix missing closing parenthesis in FromTableConfig constructor calls
Pattern: new FromTableConfig("arg1", "arg2" -> new FromTableConfig("arg1", "arg2")
"""
import os
import re
import glob

def fix_test_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original = content
    
    # Fix: Add missing closing paren for FromTableConfig constructor
    # Match: new FromTableConfig("...", "...") without closing )
    content = re.sub(
        r'new\s+FromTableConfig\(\s*"([^"]+)"\s*,\s*"([^"]+)"\s*(?!\))',
        r'new FromTableConfig("\1", "\2")',
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
    total_count = 0
    
    for filepath in glob.glob(os.path.join(base_path, "**/*Test.java"), recursive=True):
        if "FromTableConfigTest" in filepath or "DuckDbOperator" in filepath or "SqlAliasResolver" in filepath:
            continue
            
        total_count += 1
        if fix_test_file(filepath):
            fixed_count += 1
    
    print(f"\n📊 Summary: {fixed_count}/{total_count} files fixed")
