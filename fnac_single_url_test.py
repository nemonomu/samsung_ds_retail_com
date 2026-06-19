import re
import sys
import time

from playwright.sync_api import sync_playwright


URL = "https://www.fnac.com/Disque-SSD-interne-Crucial-P3-Plus-M-2-NVMe-4-To-Noir/a17432054/w-4"


def clean_text(value):
    if value is None:
        return None
    value = re.sub(r"\s+", " ", str(value)).strip()
    return value or None


def parse_price(value):
    value = clean_text(value)
    if not value:
        return None
    match = re.search(r"(\d[\d\s.,]*)", value)
    if not match:
        return None
    raw = match.group(1).replace("\u00a0", " ").replace(" ", "")
    if "," in raw and "." in raw:
        raw = raw.replace(".", "").replace(",", ".")
    else:
        raw = raw.replace(",", ".")
    try:
        return float(raw)
    except ValueError:
        return value


def first_text(page, selectors):
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            text = clean_text(locator.text_content(timeout=3000))
            if text:
                return text
        except Exception:
            continue
    return None


def first_attr(page, selectors, attrs):
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            for attr in attrs:
                value = clean_text(locator.get_attribute(attr, timeout=3000))
                if value:
                    return value
        except Exception:
            continue
    return None


def extract_from_dom(page):
    return page.evaluate(
        """
        () => {
            const clean = value => (value || '').replace(/\\s+/g, ' ').trim();

            const jsonLdProducts = [];
            for (const script of document.querySelectorAll('script[type="application/ld+json"]')) {
                try {
                    const data = JSON.parse(script.textContent || '{}');
                    const items = Array.isArray(data) ? data : [data];
                    for (const item of items) {
                        if (item && item['@type'] === 'Product') jsonLdProducts.push(item);
                    }
                } catch (e) {}
            }
            const product = jsonLdProducts[0] || {};

            let digitalData = null;
            try {
                const node = document.querySelector('#digitalData');
                digitalData = node ? JSON.parse(node.textContent || '{}') : null;
            } catch (e) {}
            const digitalProduct = digitalData && digitalData.product && digitalData.product[0];

            let tcVars = null;
            try {
                const match = document.documentElement.innerHTML.match(/var\\s+tc_vars\\s*=\\s*(\\{.*?\\})<\\/script>/s);
                tcVars = match ? JSON.parse(match[1]) : null;
            } catch (e) {}

            const title =
                clean(document.querySelector('.f-productHeader__heading')?.textContent) ||
                clean(document.querySelector('h1[data-automation-id="product-title-label"]')?.textContent) ||
                clean(document.querySelector('#FnacContent h1')?.textContent) ||
                clean(product.name) ||
                clean(digitalProduct?.productInfo?.productName) ||
                clean(document.title).split(' - ')[0];

            const imageNode =
                document.querySelector('.f-productMedias__viewItem--main') ||
                document.querySelector('#FnacContent section[class*="f-productMedias"] img') ||
                document.querySelector('#FnacContent img[src*="fnac-static.com"]');
            let imageurl =
                clean(imageNode?.getAttribute('data-zoom')) ||
                clean(imageNode?.getAttribute('src')) ||
                clean(imageNode?.getAttribute('data-src'));
            if (!imageurl && product.image) {
                imageurl = Array.isArray(product.image) ? product.image[0] : product.image;
            }
            if (!imageurl && tcVars) imageurl = clean(tcVars.product_picture_url);

            const priceText =
                clean(document.querySelector('.f-faPriceBox__price')?.textContent) ||
                clean(document.querySelector('[data-automation-id*="price"]')?.textContent);
            const price =
                priceText ||
                product.offers?.price ||
                digitalProduct?.price?.priceWithTax ||
                digitalProduct?.price?.basePriceWithTax ||
                tcVars?.product_unitprice;

            return {title, price, imageurl};
        }
        """
    )


def main():
    headless = "--headless" in sys.argv

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="fr-FR",
            timezone_id="Europe/Paris",
        )
        page = context.new_page()
        try:
            page.goto(URL, wait_until="domcontentloaded", timeout=30000)
            time.sleep(5)

            title = first_text(page, [
                ".f-productHeader__heading",
                "h1[data-automation-id='product-title-label']",
                "#FnacContent h1",
                "h1",
            ])
            imageurl = first_attr(page, [
                ".f-productMedias__viewItem--main",
                "#FnacContent section[class*='f-productMedias'] img",
                "#FnacContent img[src*='fnac-static.com']",
            ], ["data-zoom", "src", "data-src"])
            price_text = first_text(page, [
                ".f-faPriceBox__price",
                "[data-automation-id*='price']",
            ])

            dom = extract_from_dom(page)
            title = title or clean_text(dom.get("title"))
            imageurl = imageurl or clean_text(dom.get("imageurl"))
            price = parse_price(price_text) or parse_price(dom.get("price"))

            print(f"title={title}")
            print(f"price={price}")
            print(f"imageurl={imageurl}")
        finally:
            context.close()
            browser.close()


if __name__ == "__main__":
    main()
