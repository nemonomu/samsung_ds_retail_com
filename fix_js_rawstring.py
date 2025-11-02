# -*- coding: utf-8 -*-
"""
JavaScript 코드가 포함된 execute_script를 raw string으로 변경
"""
import os
import re

v2_files = [
    'fr_v2.py', 'de_v2.py', 'it_v2.py', 'au_v2.py', 'in_v2.py', 'es_v2.py',
    'uk_v2.py', 'jp_v2.py', 'usa_v2.py'
]

for filename in v2_files:
    if not os.path.exists(filename):
        print(f"[SKIP] {filename} not found")
        continue

    print(f"\n{'='*60}")
    print(f"Processing {filename}...")
    print(f"{'='*60}")

    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()

    original_content = content

    # execute_script(""" -> execute_script(r"""
    content = re.sub(
        r'execute_script\("""',
        r'execute_script(r"""',
        content
    )

    if content != original_content:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"[OK] {filename} updated")

        # 변경된 개수 확인
        count = content.count('execute_script(r"""')
        print(f"    → {count} execute_script calls converted to raw string")
    else:
        print(f"[NO CHANGE] {filename}")

print("\n" + "="*60)
print("All files processed!")
print("="*60)
