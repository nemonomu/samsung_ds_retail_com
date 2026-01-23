-- de_v2.py 선택자 DB 이관 INSERT 문
-- amazon_selectors 테이블에 독일(de) 선택자 추가

-- price 선택자 (17개)
INSERT INTO amazon_selectors (country_code, element_type, selector_value, priority, is_active) VALUES
('de', 'price', '//*[@id=''corePriceDisplay_desktop_feature_div'']/div[1]/span[1]', 1, TRUE),
('de', 'price', '//*[@id=''corePrice_feature_div'']/div/div/div/div/span[1]/span[1]', 2, TRUE),
('de', 'price', '//*[@id=''corePrice_feature_div'']/div/div/span[1]/span[1]', 3, TRUE),
('de', 'price', '//*[@id=''centerCol'']//*[@id=''corePrice_feature_div'']//span[@class=''a-offscreen'']', 4, TRUE),
('de', 'price', '//*[@id=''centerCol'']//*[@id=''corePriceDisplay_desktop_feature_div'']//span[@class=''a-offscreen'']', 5, TRUE),
('de', 'price', '//*[@id=''centerCol'']//*[@id=''apex_desktop'']//span[@class=''a-price'']//span[@class=''a-offscreen'']', 6, TRUE),
('de', 'price', '//*[@id=''centerCol'']//span[@class=''a-offscreen'']', 7, TRUE),
('de', 'price', '(//*[@id=''centerCol'']//span[@class=''a-price'']//span[@class=''a-offscreen''])[1]', 8, TRUE),
('de', 'price', '(//*[@id=''centerCol'']//span[@class=''a-price-whole''])[1]', 9, TRUE),
('de', 'price', '//*[@id=''centerCol'']//*[@id=''priceblock_ourprice'']', 10, TRUE),
('de', 'price', '//*[@id=''centerCol'']//*[@id=''priceblock_dealprice'']', 11, TRUE),
('de', 'price', '//*[@id=''centerCol'']//*[@id=''listPrice'']', 12, TRUE),
('de', 'price', '//*[@id=''centerCol'']//*[@id=''corePrice_feature_div'']//span[@class=''a-price-whole'']', 13, TRUE),
('de', 'price', '//*[@id=''centerCol'']//*[@id=''corePriceDisplay_desktop_feature_div'']//span[@class=''a-price-whole'']', 14, TRUE),
('de', 'price', '//*[@id=''centerCol'']//*[@id=''apex_desktop'']//span[@class=''a-price-whole'']', 15, TRUE),
('de', 'price', '//div[@id=''centerCol'']//span[@class=''a-price'']//span[@class=''a-offscreen'']', 16, TRUE),
('de', 'price', '//div[@id=''centerCol'']//span[@class=''a-price-whole'']', 17, TRUE);

-- price_used 선택자 (3개)
INSERT INTO amazon_selectors (country_code, element_type, selector_value, priority, is_active) VALUES
('de', 'price_used', '//*[@id=''centerCol'']//*[@id=''usedBuySection'']/div[1]/div/span[2]', 1, TRUE),
('de', 'price_used', '//*[@id=''centerCol'']//*[@id=''usedBuySection'']//span[@class=''a-offscreen'']', 2, TRUE),
('de', 'price_used', '//div[@id=''centerCol'']//div[@id=''usedBuySection'']//span[@class=''a-price'']//span[@class=''a-offscreen'']', 3, TRUE);

-- price_fraction 선택자 (4개)
INSERT INTO amazon_selectors (country_code, element_type, selector_value, priority, is_active) VALUES
('de', 'price_fraction', '//*[@id=''centerCol'']//*[@id=''corePrice_feature_div'']//span[@class=''a-price-fraction'']', 1, TRUE),
('de', 'price_fraction', '//*[@id=''centerCol'']//*[@id=''corePriceDisplay_desktop_feature_div'']//span[@class=''a-price-fraction'']', 2, TRUE),
('de', 'price_fraction', '//*[@id=''centerCol'']//*[@id=''apex_desktop'']//span[@class=''a-price-fraction'']', 3, TRUE),
('de', 'price_fraction', '//div[@id=''centerCol'']//span[@class=''a-price-fraction'']', 4, TRUE);

-- title 선택자 (3개)
INSERT INTO amazon_selectors (country_code, element_type, selector_value, priority, is_active) VALUES
('de', 'title', '#productTitle', 1, TRUE),
('de', 'title', '//span[@id=''productTitle'']', 2, TRUE),
('de', 'title', '//h1/span[@id=''productTitle'']', 3, TRUE);

-- ships_from 선택자 (5개)
INSERT INTO amazon_selectors (country_code, element_type, selector_value, priority, is_active) VALUES
('de', 'ships_from', '//*[@id=''SSOFpopoverLink_ubb'']', 1, TRUE),
('de', 'ships_from', '/html/body/div[2]/div/div/div[4]/div[1]/div[3]/div/div[1]/div/div/div/form/div/div[1]/div/div/div/div[2]/div[2]/div[3]/a', 2, TRUE),
('de', 'ships_from', '//a[@id=''SSOFpopoverLink_ubb'']', 3, TRUE),
('de', 'ships_from', '//*[@id=''fulfillerInfoFeature_feature_div'']/div[2]/div[1]/span', 4, TRUE),
('de', 'ships_from', '//div[@id=''fulfillerInfoFeature_feature_div'']//span', 5, TRUE);

-- sold_by 선택자 (5개)
INSERT INTO amazon_selectors (country_code, element_type, selector_value, priority, is_active) VALUES
('de', 'sold_by', '//a[@id=''sellerProfileTriggerId'']', 1, TRUE),
('de', 'sold_by', '//*[@id=''sellerProfileTriggerId'']', 2, TRUE),
('de', 'sold_by', '//*[@id=''merchantInfoFeature_feature_div'']/div[2]/div[1]/span', 3, TRUE),
('de', 'sold_by', '//div[@id=''merchantInfoFeature_feature_div'']//a', 4, TRUE),
('de', 'sold_by', '//div[@id=''merchantInfoFeature_feature_div'']//span', 5, TRUE);

-- imageurl 선택자 (3개)
INSERT INTO amazon_selectors (country_code, element_type, selector_value, priority, is_active) VALUES
('de', 'imageurl', '//div[@id=''imageBlock'']//img[@id=''landingImage'']', 1, TRUE),
('de', 'imageurl', '//div[@id=''main-image-container'']//img', 2, TRUE),
('de', 'imageurl', '//img[@class=''a-dynamic-image'']', 3, TRUE);
