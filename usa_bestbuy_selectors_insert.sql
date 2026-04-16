-- BestBuy USA 선택자 추가 (2026-02-06)
-- 웹사이트 구조 변경으로 인한 클래스 기반 선택자 추가

-- =====================================================
-- price 선택자 (클래스 기반 - 구조 변경에 강함)
-- =====================================================
INSERT INTO mall_selectors (mall_name, country_code, element_type, selector_value, priority, is_active) VALUES
('bestbuy', 'usa', 'price', '//span[contains(@class, "text-7") and contains(@class, "leading-7")]', 100, TRUE),
('bestbuy', 'usa', 'price', '//span[contains(@class, "font-500") and contains(@class, "text-7")]', 99, TRUE),
('bestbuy', 'usa', 'price', '//div[@data-testid="customer-price"]//span', 98, TRUE),
('bestbuy', 'usa', 'price', '//div[@data-testid="price-block-customer-price"]//span', 97, TRUE);

-- price fallback 선택자 (기존 하드코딩 → DB 이관, 2026-04-16)
INSERT INTO mall_selectors (mall_name, country_code, element_type, selector_value, priority, is_active) VALUES
('bestbuy', 'usa', 'price', '/html/body/div[6]/div[5]/div[1]/div/div[4]/div/div/div/div[1]/div/div[1]/div[1]/div[1]/div/div/div/div[1]/span', 90, TRUE),
('bestbuy', 'usa', 'price', '/html/body/div[5]/div[4]/div[2]/div/div[3]/div/div/div[1]/div/div[1]/div[2]/div[1]/div/div/div/div[1]', 89, TRUE),
('bestbuy', 'usa', 'price', '/html/body/div[5]/div[4]/div[2]/div/div[3]/div/div/div[1]/div/div[1]/div[2]/div[1]/div/div/div/div[1]/span', 88, TRUE),
('bestbuy', 'usa', 'price', '/html/body/div[5]/div[4]/div[2]/div/div[3]/div/div/div[1]/div/div[1]/div[1]/div[1]/div/div/div/div[1]/span', 87, TRUE),
('bestbuy', 'usa', 'price', '/html/body/div[5]/div[4]/div[2]/div/div[4]/div/div/div[1]/div/div[1]/div[1]/div[1]/div/div/div/div[1]/span', 86, TRUE);

-- =====================================================
-- title 선택자 (클래스 기반 - 구조 변경에 강함)
-- =====================================================
INSERT INTO mall_selectors (mall_name, country_code, element_type, selector_value, priority, is_active) VALUES
('bestbuy', 'usa', 'title', '//h1[contains(@class, "h4")]', 100, TRUE);

-- title fallback 선택자 (기존 하드코딩 → DB 이관, 2026-04-16)
INSERT INTO mall_selectors (mall_name, country_code, element_type, selector_value, priority, is_active) VALUES
('bestbuy', 'usa', 'title', '/html/body/div[6]/div[5]/div[1]/div/div[2]/h1', 90, TRUE),
('bestbuy', 'usa', 'title', '/html/body/div[5]/div[4]/div[1]/div/h1', 89, TRUE),
('bestbuy', 'usa', 'title', '/html/body/div[5]/div[4]/div[2]/div/h1', 88, TRUE),
('bestbuy', 'usa', 'title', '//h1[@class="sku-title"]', 87, TRUE),
('bestbuy', 'usa', 'title', '//div[@class="sku-title"]//h1', 86, TRUE);

-- =====================================================
-- imageurl 선택자 (2026-04-16)
-- =====================================================
INSERT INTO mall_selectors (mall_name, country_code, element_type, selector_value, priority, is_active) VALUES
('bestbuy', 'usa', 'imageurl', '/html/body/div[6]/div[8]/div[2]/div/div[2]/div/div[2]/div/div/div[2]/div/button[1]/img', 100, TRUE);
