INSERT INTO samsung_ds_retail_com.amazon_selectors (country_code, element_type, selector_value, priority, is_active)
SELECT 'de', 'price',
       '(//*[@id=''centerCol'']//*[@id=''corePriceDisplay_desktop_feature_div'']//span[contains(@class,''a-price'') and not(contains(@class,''a-text-price''))][1])[1]',
       130, TRUE
WHERE NOT EXISTS (
    SELECT 1 FROM samsung_ds_retail_com.amazon_selectors
    WHERE country_code = 'de'
      AND element_type = 'price'
      AND selector_value = '(//*[@id=''centerCol'']//*[@id=''corePriceDisplay_desktop_feature_div'']//span[contains(@class,''a-price'') and not(contains(@class,''a-text-price''))][1])[1]'
);

INSERT INTO samsung_ds_retail_com.amazon_selectors (country_code, element_type, selector_value, priority, is_active)
SELECT 'de', 'price',
       '(//*[@id=''centerCol'']//*[@id=''corePrice_feature_div'']//span[contains(@class,''a-price'') and not(contains(@class,''a-text-price''))][1])[1]',
       129, TRUE
WHERE NOT EXISTS (
    SELECT 1 FROM samsung_ds_retail_com.amazon_selectors
    WHERE country_code = 'de'
      AND element_type = 'price'
      AND selector_value = '(//*[@id=''centerCol'']//*[@id=''corePrice_feature_div'']//span[contains(@class,''a-price'') and not(contains(@class,''a-text-price''))][1])[1]'
);

INSERT INTO samsung_ds_retail_com.amazon_selectors (country_code, element_type, selector_value, priority, is_active)
SELECT 'de', 'ships_from',
       '(//div[@id=''fulfillerInfoFeature_feature_div'']//span[contains(@class,''offer-display-feature-text-message'') and normalize-space(.)!=''''])[1]',
       130, TRUE
WHERE NOT EXISTS (
    SELECT 1 FROM samsung_ds_retail_com.amazon_selectors
    WHERE country_code = 'de'
      AND element_type = 'ships_from'
      AND selector_value = '(//div[@id=''fulfillerInfoFeature_feature_div'']//span[contains(@class,''offer-display-feature-text-message'') and normalize-space(.)!=''''])[1]'
);

INSERT INTO samsung_ds_retail_com.amazon_selectors (country_code, element_type, selector_value, priority, is_active)
SELECT 'de', 'ships_from',
       '(//div[@id=''usedOnlyLayoutFulfillerInfoFeature_feature_div'']//span[contains(@class,''offer-display-feature-text-message'') and normalize-space(.)!=''''])[1]',
       129, TRUE
WHERE NOT EXISTS (
    SELECT 1 FROM samsung_ds_retail_com.amazon_selectors
    WHERE country_code = 'de'
      AND element_type = 'ships_from'
      AND selector_value = '(//div[@id=''usedOnlyLayoutFulfillerInfoFeature_feature_div'']//span[contains(@class,''offer-display-feature-text-message'') and normalize-space(.)!=''''])[1]'
);

INSERT INTO samsung_ds_retail_com.amazon_selectors (country_code, element_type, selector_value, priority, is_active)
SELECT 'de', 'ships_from',
       '(//div[@id=''merchantInfoFeature_feature_div''][.//div[contains(@class,''offer-display-feature-label'')][(contains(normalize-space(.),''Versender'') and contains(normalize-space(.),''Verk'')) or (contains(normalize-space(.),''Shipper'') and contains(normalize-space(.),''Seller''))]]//div[contains(@class,''offer-display-feature-text'')]//a[@id=''sellerProfileTriggerId'' and normalize-space(.)!=''''])[1]',
       128, TRUE
WHERE NOT EXISTS (
    SELECT 1 FROM samsung_ds_retail_com.amazon_selectors
    WHERE country_code = 'de'
      AND element_type = 'ships_from'
      AND selector_value = '(//div[@id=''merchantInfoFeature_feature_div''][.//div[contains(@class,''offer-display-feature-label'')][(contains(normalize-space(.),''Versender'') and contains(normalize-space(.),''Verk'')) or (contains(normalize-space(.),''Shipper'') and contains(normalize-space(.),''Seller''))]]//div[contains(@class,''offer-display-feature-text'')]//a[@id=''sellerProfileTriggerId'' and normalize-space(.)!=''''])[1]'
);

INSERT INTO samsung_ds_retail_com.amazon_selectors (country_code, element_type, selector_value, priority, is_active)
SELECT 'de', 'ships_from',
       '(//div[@id=''usedOnlyLayoutMerchantInfoFeature_feature_div''][.//div[contains(@class,''offer-display-feature-label'')][(contains(normalize-space(.),''Versender'') and contains(normalize-space(.),''Verk'')) or (contains(normalize-space(.),''Shipper'') and contains(normalize-space(.),''Seller''))]]//div[contains(@class,''offer-display-feature-text'')]//a[@id=''sellerProfileTriggerId'' and normalize-space(.)!=''''])[1]',
       127, TRUE
WHERE NOT EXISTS (
    SELECT 1 FROM samsung_ds_retail_com.amazon_selectors
    WHERE country_code = 'de'
      AND element_type = 'ships_from'
      AND selector_value = '(//div[@id=''usedOnlyLayoutMerchantInfoFeature_feature_div''][.//div[contains(@class,''offer-display-feature-label'')][(contains(normalize-space(.),''Versender'') and contains(normalize-space(.),''Verk'')) or (contains(normalize-space(.),''Shipper'') and contains(normalize-space(.),''Seller''))]]//div[contains(@class,''offer-display-feature-text'')]//a[@id=''sellerProfileTriggerId'' and normalize-space(.)!=''''])[1]'
);

INSERT INTO samsung_ds_retail_com.amazon_selectors (country_code, element_type, selector_value, priority, is_active)
SELECT 'de', 'ships_from',
       '(//div[@id=''shipFromSoldByAbbreviated_feature_div'']//span[contains(normalize-space(.),''Versand durch'')]/following-sibling::span[normalize-space(.)!=''''][1])[1]',
       126, TRUE
WHERE NOT EXISTS (
    SELECT 1 FROM samsung_ds_retail_com.amazon_selectors
    WHERE country_code = 'de'
      AND element_type = 'ships_from'
      AND selector_value = '(//div[@id=''shipFromSoldByAbbreviated_feature_div'']//span[contains(normalize-space(.),''Versand durch'')]/following-sibling::span[normalize-space(.)!=''''][1])[1]'
);

INSERT INTO samsung_ds_retail_com.amazon_selectors (country_code, element_type, selector_value, priority, is_active)
SELECT 'de', 'sold_by',
       '(//div[@id=''used_buybox_desktop'']//div[contains(@class,''a-row'')][contains(normalize-space(.),''Verkauft von'')]//a[@id=''sellerProfileTriggerId'' and normalize-space(.)!=''''])[1]',
       130, TRUE
WHERE NOT EXISTS (
    SELECT 1 FROM samsung_ds_retail_com.amazon_selectors
    WHERE country_code = 'de'
      AND element_type = 'sold_by'
      AND selector_value = '(//div[@id=''used_buybox_desktop'']//div[contains(@class,''a-row'')][contains(normalize-space(.),''Verkauft von'')]//a[@id=''sellerProfileTriggerId'' and normalize-space(.)!=''''])[1]'
);

INSERT INTO samsung_ds_retail_com.amazon_selectors (country_code, element_type, selector_value, priority, is_active)
SELECT 'de', 'sold_by',
       '(//div[@id=''merchantInfoFeature_feature_div'']//a[@id=''sellerProfileTriggerId'' and normalize-space(.)!=''''])[1]',
       129, TRUE
WHERE NOT EXISTS (
    SELECT 1 FROM samsung_ds_retail_com.amazon_selectors
    WHERE country_code = 'de'
      AND element_type = 'sold_by'
      AND selector_value = '(//div[@id=''merchantInfoFeature_feature_div'']//a[@id=''sellerProfileTriggerId'' and normalize-space(.)!=''''])[1]'
);

INSERT INTO samsung_ds_retail_com.amazon_selectors (country_code, element_type, selector_value, priority, is_active)
SELECT 'de', 'sold_by',
       '(//div[@id=''usedOnlyLayoutMerchantInfoFeature_feature_div'']//a[@id=''sellerProfileTriggerId'' and normalize-space(.)!=''''])[1]',
       128, TRUE
WHERE NOT EXISTS (
    SELECT 1 FROM samsung_ds_retail_com.amazon_selectors
    WHERE country_code = 'de'
      AND element_type = 'sold_by'
      AND selector_value = '(//div[@id=''usedOnlyLayoutMerchantInfoFeature_feature_div'']//a[@id=''sellerProfileTriggerId'' and normalize-space(.)!=''''])[1]'
);

SELECT country_code, element_type, selector_value, priority, is_active
FROM samsung_ds_retail_com.amazon_selectors
WHERE country_code = 'de'
  AND element_type IN ('price', 'ships_from', 'sold_by')
  AND priority >= 126
ORDER BY element_type, priority DESC;
