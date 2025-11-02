# -*- coding: utf-8 -*-
"""
잘못된 f-string 이스케이프 수정
f\"{...}\" -> f"{...}"
"""
import os

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

    # f\"{now_time.microsecond:06d}\" -> f"{now_time.microsecond:06d}"
    content = content.replace(
        r'f\"{now_time.microsecond:06d}\"',
        'f"{now_time.microsecond:06d}"'
    )

    # f\"{local_time.microsecond:06d}\" -> f"{local_time.microsecond:06d}" (혹시 있을 경우)
    content = content.replace(
        r'f\"{local_time.microsecond:06d}\"',
        'f"{local_time.microsecond:06d}"'
    )

    if content != original_content:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"[OK] {filename} updated")
    else:
        print(f"[NO CHANGE] {filename}")

print("\n" + "="*60)
print("All files processed!")
print("="*60)
