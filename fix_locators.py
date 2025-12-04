import re

with open('fnac.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Pattern 1: locator = self.page.locator(f'xpath={selector}')
# Replace with: elements = self.driver.find_elements(By.XPATH, selector)
content = re.sub(
    r"locator = self\.page\.locator\(f'xpath=\{selector\}'\)",
    r"elements = self.driver.find_elements(By.XPATH, selector)",
    content
)

# Pattern 2: locator = self.page.locator(selector)
# Replace with: elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
content = re.sub(
    r"(\s+)locator = self\.page\.locator\(selector\)",
    r"\1elements = self.driver.find_elements(By.CSS_SELECTOR, selector)",
    content
)

# Pattern 3: locator.wait_for(state='visible', timeout=5000) -> remove or simplify
content = re.sub(
    r"locator\.wait_for\(state='visible', timeout=\d+\)\s*\n",
    r"",
    content
)

# Pattern 4: locator.inner_text() -> elements[0].text if elements else None
content = re.sub(
    r"(\s+)title_text = locator\.inner_text\(\)",
    r"\1title_text = elements[0].text if elements and elements[0].is_displayed() else None",
    content
)

# Pattern 5: price_value = locator.inner_text()
content = re.sub(
    r"(\s+)price_value = locator\.inner_text\(\)",
    r"\1price_value = elements[0].text if elements and elements[0].is_displayed() else None",
    content
)

# Pattern 6: image_url = locator.get_attribute('src')
content = re.sub(
    r"(\s+)image_url = locator\.get_attribute\('src'\)",
    r"\1image_url = elements[0].get_attribute('src') if elements and elements[0].is_displayed() else None",
    content
)

with open('fnac.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Locator conversion completed")
