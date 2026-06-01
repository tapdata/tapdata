#!/usr/bin/env python3
"""
Comprehensive fix for FromTableConfig constructor calls in test files
Fixes multiple patterns of missing closing parentheses
"""
import os
import re
import glob

def fix_test_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original = content
    
    # Pattern 1: fromTables.add(new FromTableConfig("...", "...");
    # Fix: Add missing ) before ;
    content = re.sub(
        r'(\.\s*add\s*\(\s*new\s+FromTableConfig\([^)]+\))(?=\s*;)',
        r'\1)',
        content
    )
    
    # Pattern 2: Collections.singletonList(new FromTableConfig("...", "..."
    # Fix: Add missing )) at end
    content = re.sub(
        r'(Collections\.singletonList\(\s*new\s+FromTableConfig\([^)]+)(?=\s*\))',
        r'\1)',
        content
    )
    
    # Pattern 3: Arrays.asList(new FromTableConfig(...), new FromTableConfig(... 
    # with missing closing paren on last element
    content = re.sub(
        r'(new\s+FromTableConfig\([^)]+\))\s*,\s*(new\s+FromTableConfig\([^)]+)(?=\s*\))',
        r'\1, \2)',
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
    
    target_files = [
        "scenarios/EdgeCasesScenariosTest.java",
        "scenarios/FromTableScenariosTest.java",
        "scenarios/ABAProblemScenariosTest.java",
        "scenarios/BatchBoundaryScenariosTest.java",
        "scenarios/JoinKeyUpdateScenariosTest.java",
        "scenarios/MainTableScenariosTest.java",
        "BatchWideTableUpdateIntegrationTest.java",
        "WithCteIntegrationTest.java"
    ]
    
    fixed_count = 0
    
    for filename in target_files:
        filepath = os.path.join(base_path, "**", filename)
        for matched_file in glob.glob(filepath, recursive=True):
            if fix_test_file(matched_file):
                fixed_count += 1
    
    print(f"\n📊 Summary: {fixed_count} files fixed")
