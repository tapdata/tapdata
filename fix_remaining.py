#!/usr/bin/env python3
"""
Fix remaining 4-param FromTableConfig constructor calls
Target: 14 specific errors in 8 files
"""
import os
import re

def fix_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original = content
    
    # Fix 4-param constructor: (tableName, pkField, querySql, fields) -> (preNodeId, tableNameInSql)
    # Match: new FromTableConfig("x", "y", "query", fieldsVar)
    content = re.sub(
        r'new\s+FromTableConfig\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*[^,]+,\s*\w+\)',
        r'new FromTableConfig(\1, \2)',
        content
    )
    
    # Fix double closing paren issue
    # Pattern: new FromTableConfig(...));
    # Fix:    new FromTableConfig(...))
    content = re.sub(
        r'(new\s+FromTableConfig\([^)]+\))\)\s*;\s*\)',
        r'\1);',
        content
    )
    
    if content != original:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        return True
    return False

files_to_fix = [
    "/Users/hj/workspace/tapdata/iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/AffectedKeyCalculatorMergedRecordsTest.java",
    "/Users/hj/workspace/tapdata/iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/scenarios/ABAProblemScenariosTest.java",
    "/Users/hj/workspace/tapdata/iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/scenarios/BatchBoundaryScenariosTest.java",
    "/Users/hj/workspace/tapdata/iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/scenarios/EdgeCasesScenariosTest.java",
    "/Users/hj/workspace/tapdata/iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/scenarios/FromTableScenariosTest.java",
    "/Users/hj/workspace/tapdata/iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/scenarios/JoinKeyUpdateScenariosTest.java",
    "/Users/hj/workspace/tapdata/iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/scenarios/MainTableScenariosTest.java"
]

fixed = 0
for f in files_to_fix:
    if os.path.exists(f):
        if fix_file(f):
            print(f"✅ {os.path.basename(f)}")
            fixed += 1
        else:
            print(f"⏭️  {os.path.basename(f)}")

print(f"\n📊 Fixed {fixed}/{len(files_to_fix)} files")
