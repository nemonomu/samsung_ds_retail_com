/*
Amazon DE buybox offer selectors for combined shipper/seller labels.

Why these are needed:
de_v2.py handled "Versender / Verkaeufer" with code-level fallback.
de_v3.py intentionally reads ships_from/sold_by paths from amazon_selectors,
so the same combined-label paths must exist in the DB.

Verified against saved HTML in:
references/verifying/de_v3_run_20260528170007
- B087DFFJRD: merchantInfoFeature combined label -> Hype Network
- B08GTYFC37: merchantInfoFeature combined label -> Amazon
- B0CTRVZKG7: usedOnlyLayoutMerchantInfoFeature combined label -> GER-POL
- B08PC5ZYB1: usedOnlyLayoutMerchantInfoFeature combined label -> EinfachOrangensaftShop
- B0CTS93WML: usedOnlyLayoutFulfillerInfoFeature -> Amazon
- B0CCN9NHTC/B0CK2S298S/B0CTRXBKHP/B0F3GGX4SK: fulfillerInfoFeature -> Amazon
*/

INSERT INTO samsung_ds_retail_com.amazon_selectors
    (country_code, element_type, selector_value, priority, is_active)
SELECT
    'de',
    'ships_from',
    '(//div[@id=''merchantInfoFeature_feature_div''][.//*[contains(normalize-space(.),''Versender'') and contains(normalize-space(.),''Verk'')]]//*[contains(@class,''offer-display-feature-text-message'') and normalize-space(.)!=''''])[1]',
    126,
    TRUE
WHERE NOT EXISTS (
    SELECT 1
    FROM samsung_ds_retail_com.amazon_selectors
    WHERE country_code = 'de'
      AND element_type = 'ships_from'
      AND selector_value = '(//div[@id=''merchantInfoFeature_feature_div''][.//*[contains(normalize-space(.),''Versender'') and contains(normalize-space(.),''Verk'')]]//*[contains(@class,''offer-display-feature-text-message'') and normalize-space(.)!=''''])[1]'
);

INSERT INTO samsung_ds_retail_com.amazon_selectors
    (country_code, element_type, selector_value, priority, is_active)
SELECT
    'de',
    'sold_by',
    '(//div[@id=''merchantInfoFeature_feature_div''][.//*[contains(normalize-space(.),''Versender'') and contains(normalize-space(.),''Verk'')]]//*[contains(@class,''offer-display-feature-text-message'') and normalize-space(.)!=''''])[1]',
    126,
    TRUE
WHERE NOT EXISTS (
    SELECT 1
    FROM samsung_ds_retail_com.amazon_selectors
    WHERE country_code = 'de'
      AND element_type = 'sold_by'
      AND selector_value = '(//div[@id=''merchantInfoFeature_feature_div''][.//*[contains(normalize-space(.),''Versender'') and contains(normalize-space(.),''Verk'')]]//*[contains(@class,''offer-display-feature-text-message'') and normalize-space(.)!=''''])[1]'
);

INSERT INTO samsung_ds_retail_com.amazon_selectors
    (country_code, element_type, selector_value, priority, is_active)
SELECT
    'de',
    'ships_from',
    '(//div[@id=''fulfillerInfoFeature_feature_div'']//*[contains(@class,''offer-display-feature-text-message'') and normalize-space(.)!=''''])[1]',
    125,
    TRUE
WHERE NOT EXISTS (
    SELECT 1
    FROM samsung_ds_retail_com.amazon_selectors
    WHERE country_code = 'de'
      AND element_type = 'ships_from'
      AND selector_value = '(//div[@id=''fulfillerInfoFeature_feature_div'']//*[contains(@class,''offer-display-feature-text-message'') and normalize-space(.)!=''''])[1]'
);

INSERT INTO samsung_ds_retail_com.amazon_selectors
    (country_code, element_type, selector_value, priority, is_active)
SELECT
    'de',
    'ships_from',
    '(//div[@id=''usedOnlyLayoutFulfillerInfoFeature_feature_div'']//*[contains(@class,''offer-display-feature-text-message'') and normalize-space(.)!=''''])[1]',
    124,
    TRUE
WHERE NOT EXISTS (
    SELECT 1
    FROM samsung_ds_retail_com.amazon_selectors
    WHERE country_code = 'de'
      AND element_type = 'ships_from'
      AND selector_value = '(//div[@id=''usedOnlyLayoutFulfillerInfoFeature_feature_div'']//*[contains(@class,''offer-display-feature-text-message'') and normalize-space(.)!=''''])[1]'
);

INSERT INTO samsung_ds_retail_com.amazon_selectors
    (country_code, element_type, selector_value, priority, is_active)
SELECT
    'de',
    'ships_from',
    '(//div[@id=''usedOnlyLayoutMerchantInfoFeature_feature_div''][.//*[contains(normalize-space(.),''Versender'') and contains(normalize-space(.),''Verk'')]]//*[contains(@class,''offer-display-feature-text-message'') and normalize-space(.)!=''''])[1]',
    123,
    TRUE
WHERE NOT EXISTS (
    SELECT 1
    FROM samsung_ds_retail_com.amazon_selectors
    WHERE country_code = 'de'
      AND element_type = 'ships_from'
      AND selector_value = '(//div[@id=''usedOnlyLayoutMerchantInfoFeature_feature_div''][.//*[contains(normalize-space(.),''Versender'') and contains(normalize-space(.),''Verk'')]]//*[contains(@class,''offer-display-feature-text-message'') and normalize-space(.)!=''''])[1]'
);

INSERT INTO samsung_ds_retail_com.amazon_selectors
    (country_code, element_type, selector_value, priority, is_active)
SELECT
    'de',
    'sold_by',
    '(//div[@id=''usedOnlyLayoutMerchantInfoFeature_feature_div''][.//*[contains(normalize-space(.),''Versender'') and contains(normalize-space(.),''Verk'')]]//*[contains(@class,''offer-display-feature-text-message'') and normalize-space(.)!=''''])[1]',
    123,
    TRUE
WHERE NOT EXISTS (
    SELECT 1
    FROM samsung_ds_retail_com.amazon_selectors
    WHERE country_code = 'de'
      AND element_type = 'sold_by'
      AND selector_value = '(//div[@id=''usedOnlyLayoutMerchantInfoFeature_feature_div''][.//*[contains(normalize-space(.),''Versender'') and contains(normalize-space(.),''Verk'')]]//*[contains(@class,''offer-display-feature-text-message'') and normalize-space(.)!=''''])[1]'
);

SELECT country_code, element_type, selector_value, priority, is_active
FROM samsung_ds_retail_com.amazon_selectors
WHERE country_code = 'de'
  AND element_type IN ('ships_from', 'sold_by')
  AND (
      selector_value LIKE '%merchantInfoFeature_feature_div%'
      OR selector_value LIKE '%FulfillerInfoFeature_feature_div%'
  )
ORDER BY element_type, priority DESC;
