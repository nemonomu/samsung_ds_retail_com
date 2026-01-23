-- fr_v2.py 선택자 DB 마이그레이션 INSERT 문
-- 실행 전 기존 fr 선택자 확인: SELECT * FROM amazon_selectors WHERE country_code = 'fr';

-- =====================================================
-- price 선택자 (21개)
-- =====================================================
INSERT INTO amazon_selectors (country_code, element_type, selector_value, priority, is_active) VALUES
('fr', 'price', '//*[@id=''corePriceDisplay_desktop_feature_div'']/div[1]/span[1]', 1, TRUE),
('fr', 'price', '//*[@id=''corePrice_feature_div'']/div/div/div/div/span[1]/span[1]', 2, TRUE),
('fr', 'price', '//*[@id=''corePrice_feature_div'']/div/div/span[1]/span[1]', 3, TRUE),
('fr', 'price', '//*[@id=''centerCol'']//*[@id=''corePrice_feature_div'']//span[@class=''a-offscreen'']', 4, TRUE),
('fr', 'price', '//*[@id=''centerCol'']//*[@id=''corePriceDisplay_desktop_feature_div'']//span[@class=''a-offscreen'']', 5, TRUE),
('fr', 'price', '//*[@id=''centerCol'']//*[@id=''apex_desktop'']//span[@class=''a-price'']//span[@class=''a-offscreen'']', 6, TRUE),
('fr', 'price', '//*[@id=''centerCol'']//span[@class=''a-offscreen'']', 7, TRUE),
('fr', 'price', '(//*[@id=''centerCol'']//span[@class=''a-price'']//span[@class=''a-offscreen''])[1]', 8, TRUE),
('fr', 'price', '(//*[@id=''centerCol'']//span[@class=''a-price-whole''])[1]', 9, TRUE),
('fr', 'price', '//*[@id=''centerCol'']//*[@id=''priceblock_ourprice'']', 10, TRUE),
('fr', 'price', '//*[@id=''centerCol'']//*[@id=''priceblock_dealprice'']', 11, TRUE),
('fr', 'price', '//*[@id=''centerCol'']//*[@id=''listPrice'']', 12, TRUE),
('fr', 'price', '//*[@id=''centerCol'']//*[@id=''corePrice_feature_div'']//span[@class=''a-price-whole'']', 13, TRUE),
('fr', 'price', '//*[@id=''centerCol'']//*[@id=''corePriceDisplay_desktop_feature_div'']//span[@class=''a-price-whole'']', 14, TRUE),
('fr', 'price', '//*[@id=''centerCol'']//*[@id=''apex_desktop'']//span[@class=''a-price-whole'']', 15, TRUE),
('fr', 'price', '//*[@id=''centerCol'']//*[@id=''usedBuySection'']/div[1]/div/span[2]', 16, TRUE),
('fr', 'price', '//*[@id=''centerCol'']//*[@id=''usedBuySection'']//span[@class=''a-offscreen'']', 17, TRUE),
('fr', 'price', '/html/body/div[2]/div/div/div[4]/div[4]/div[13]/div/div/div[3]/div[1]/span[1]', 18, TRUE),
('fr', 'price', '//div[@id=''centerCol'']//span[@class=''a-price'']//span[@class=''a-offscreen'']', 19, TRUE),
('fr', 'price', '//div[@id=''centerCol'']//span[@class=''a-price-whole'']', 20, TRUE);

-- =====================================================
-- price_used 선택자 (3개)
-- =====================================================
INSERT INTO amazon_selectors (country_code, element_type, selector_value, priority, is_active) VALUES
('fr', 'price_used', '//*[@id=''centerCol'']//*[@id=''usedBuySection'']/div[1]/div/span[2]', 1, TRUE),
('fr', 'price_used', '//*[@id=''centerCol'']//*[@id=''usedBuySection'']//span[@class=''a-offscreen'']', 2, TRUE),
('fr', 'price_used', '//div[@id=''centerCol'']//div[@id=''usedBuySection'']//span[@class=''a-price'']//span[@class=''a-offscreen'']', 3, TRUE);

-- =====================================================
-- price_fraction 선택자 (4개)
-- =====================================================
INSERT INTO amazon_selectors (country_code, element_type, selector_value, priority, is_active) VALUES
('fr', 'price_fraction', '//*[@id=''centerCol'']//*[@id=''corePrice_feature_div'']//span[@class=''a-price-fraction'']', 1, TRUE),
('fr', 'price_fraction', '//*[@id=''centerCol'']//*[@id=''corePriceDisplay_desktop_feature_div'']//span[@class=''a-price-fraction'']', 2, TRUE),
('fr', 'price_fraction', '//*[@id=''centerCol'']//*[@id=''apex_desktop'']//span[@class=''a-price-fraction'']', 3, TRUE),
('fr', 'price_fraction', '//div[@id=''centerCol'']//span[@class=''a-price-fraction'']', 4, TRUE);

-- =====================================================
-- title 선택자 (3개)
-- =====================================================
INSERT INTO amazon_selectors (country_code, element_type, selector_value, priority, is_active) VALUES
('fr', 'title', '#productTitle', 1, TRUE),
('fr', 'title', '//span[@id=''productTitle'']', 2, TRUE),
('fr', 'title', '//h1/span[@id=''productTitle'']', 3, TRUE);

-- =====================================================
-- ships_from 선택자 (5개)
-- =====================================================
INSERT INTO amazon_selectors (country_code, element_type, selector_value, priority, is_active) VALUES
('fr', 'ships_from', '/html/body/div[2]/div/div/div[4]/div[1]/div[4]/div/div[1]/div/div[1]/div/div/div[1]/div/div[2]/div/form/div/div/div[21]/div/div/div[1]/div/div[2]/div[2]/div[1]/span', 1, TRUE),
('fr', 'ships_from', '//*[@id=''SSOFpopoverLink_ubb'']', 2, TRUE),
('fr', 'ships_from', '//*[@id=''fulfillerInfoFeature_feature_div'']/div[2]/div[1]/span', 3, TRUE),
('fr', 'ships_from', '//div[@id=''fulfillerInfoFeature_feature_div'']//span', 4, TRUE),
('fr', 'ships_from', '//a[@id=''SSOFpopoverLink_ubb'']', 5, TRUE);

-- =====================================================
-- excluded_ships_from_xpaths 선택자 (1개)
-- =====================================================
INSERT INTO amazon_selectors (country_code, element_type, selector_value, priority, is_active) VALUES
('fr', 'excluded_ships_from_xpaths', '/html/body/div[2]/div/div/div[4]/div[1]/div[4]/div/div[1]/div/div[1]/div/div/div[2]/div/div[1]/h5/div[4]/div/div[1]/div/span[2]', 1, TRUE);

-- =====================================================
-- sold_by 선택자 (7개)
-- =====================================================
INSERT INTO amazon_selectors (country_code, element_type, selector_value, priority, is_active) VALUES
('fr', 'sold_by', '/html/body/div[2]/div/div/div[4]/div[1]/div[4]/div/div[1]/div/div[1]/div/div/div[1]/div/div[2]/div/form/div/div/div[21]/div/div/div[1]/div/div[3]/div[2]/div[1]/span', 1, TRUE),
('fr', 'sold_by', '/html/body/div[2]/div/div/div[4]/div[1]/div[4]/div/div[1]/div/div[1]/div/div/div[1]/div/div[2]/div/form/div/div/div[21]/div/div/div[1]/div/div[3]/div[2]/div[1]/a', 2, TRUE),
('fr', 'sold_by', '//*[@id=''sellerProfileTriggerId'']', 3, TRUE),
('fr', 'sold_by', '//*[@id=''merchantInfoFeature_feature_div'']/div[2]/div[1]/span', 4, TRUE),
('fr', 'sold_by', '//div[@id=''merchantInfoFeature_feature_div'']//a', 5, TRUE),
('fr', 'sold_by', '//div[@id=''merchantInfoFeature_feature_div'']//span', 6, TRUE),
('fr', 'sold_by', '//a[@id=''sellerProfileTriggerId'']', 7, TRUE);

-- =====================================================
-- excluded_sold_by_xpaths 선택자 (1개)
-- =====================================================
INSERT INTO amazon_selectors (country_code, element_type, selector_value, priority, is_active) VALUES
('fr', 'excluded_sold_by_xpaths', '/html/body/div[2]/div/div/div[4]/div[1]/div[4]/div/div[1]/div/div[1]/div/div/div[3]/div/div[1]/h5/div[5]/div/div/div/div[3]/div[2]/div/span', 1, TRUE);

-- =====================================================
-- imageurl 선택자 (3개)
-- =====================================================
INSERT INTO amazon_selectors (country_code, element_type, selector_value, priority, is_active) VALUES
('fr', 'imageurl', '//div[@id=''imageBlock'']//img[@id=''landingImage'']', 1, TRUE),
('fr', 'imageurl', '//div[@id=''main-image-container'']//img', 2, TRUE),
('fr', 'imageurl', '//img[@class=''a-dynamic-image'']', 3, TRUE);

-- =====================================================
-- 총 50개 선택자 INSERT 완료
-- =====================================================
