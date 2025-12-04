import re

with open('fnac.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

new_lines = []
for line in lines:
    # self.page.goto() -> self.driver.get()
    line = re.sub(r'self\.page\.goto\(([^,]+),.*?\)', r'self.driver.get(\1)', line)

    # self.page.reload() -> self.driver.refresh()
    line = re.sub(r'self\.page\.reload\(.*?\)', r'self.driver.refresh()', line)

    # self.page.title() -> self.driver.title
    line = re.sub(r'self\.page\.title\(\)', r'self.driver.title', line)

    # self.page.evaluate() -> self.driver.execute_script()
    line = re.sub(r'self\.page\.evaluate\(', r'self.driver.execute_script(', line)

    new_lines.append(line)

with open('fnac.py', 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print("✅ Basic API conversion completed!")
