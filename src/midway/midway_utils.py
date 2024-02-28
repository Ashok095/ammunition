import logging
from fake_useragent import UserAgent

logger = logging.getLogger()
import json
import re


def get_family_and_object_id(url):
    pattern = r"https://www.midwayusa.com/api/product/data\?id=(.+)&pid=(.+)"
    matches = re.search(pattern, url)

    if matches:
        family_id = matches.group(1)
        pid_value = matches.group(2)
    else:
        family_id = None
        pid_value = None
    return family_id, pid_value


def convert_to_nested_dict(raw_data):
    logger.info("convert_to_nested_dict - converting breadcrumb to dictionary")
    nested_dict = {}

    for key, value in raw_data.items():
        categories = key.split(">")
        current_dict = nested_dict

        for category in categories:
            category_id, category_name = category.split(":")
            category_name = category_name.lower()

            if category_name not in current_dict:
                current_dict[category_name] = {"id": int(category_id)}
            current_dict = current_dict[category_name]

        current_dict["count"] = value
    logger.info("Returning category dictionary")
    return nested_dict


def get_count_and_id(dic, bottom_dicts):
    if all(key in ["id", "count"] for key in dic):
        bottom_dicts.append(dic)
    else:
        for key, values in dic.items():
            if isinstance(values, dict):
                get_count_and_id(values, bottom_dicts)
    return bottom_dicts


def create_api_urls_from_data(data):
    base_url = "https://www.midwayusa.com/api/product/data?id={}&pid={}"

    # Construct a list of URLs for each record in data
    url_list = [
        base_url.format(record["familyId"], record["objectID"]) for record in data
    ]

    return url_list


def get_id_count_from_response(response):
    breadcrumb = response["facets"]["categoryLevelsNew"]
    category_levels = convert_to_nested_dict(breadcrumb)
    object_id_count_list = get_count_and_id(category_levels, [])
    return object_id_count_list


def get_user_agent(os="linux"):
    if os == "linux":
        platform = "Linux x86_64"
    else:
        platform = "Windows"
    ua = UserAgent(os=os, browsers=["chrome"])
    # user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.6167.85 Safari/537.36"
    user_agent = ua.random
    return user_agent, platform


def extract_price_from_money(data):
    if not data:
        return None
    regex = r"\$\s*([\d,]+(\.\d+)?)"
    match = re.search(regex, data)
    if match:
        str_price = match.group(1)
        price = float(str_price.replace(",", ""))
    else:
        price = None
    return price


def get_price(data):
    if data:
        logger.info(f"price info - {data}")
        raw_marked_price = data.get("listPriceAmount", None)
        marked_price = extract_price_from_money(raw_marked_price)
        raw_selling_price = data.get("ourPriceAmount", None)
        selling_price = extract_price_from_money(raw_selling_price)
        raw_discount_price = data.get("discountedPriceAmount", None)
        discount_price = extract_price_from_money(raw_discount_price)
        return marked_price, selling_price, discount_price
    return None, None, None


def extract_details_from_response(response_data):
    features = {}
    products = []
    for i in response_data["productFamily"]["attributes"]:
        features[i["name"]] = i["value"]

    description = response_data["productFamily"]["blurbText"]

    # breadcrumb = response_data["productFamily"]["breadCrumbTrail"].split("/")
    # category = breadcrumb[-2] if len(breadcrumb) > 2 else breadcrumb[-1]

    category = response_data["productFamily"]["productType"]

    family_id = response_data["productFamily"].get("familyNumber", None)

    brand_name = response_data["productFamily"]["brandInformation"].get(
        "brandName", None
    )
    images = [img["path"] for img in response_data["productFamily"]["images"]]
    product_data = response_data["products"][0]

    # for i, product_data in enumerate(response_data["products"]):
    logger.info("Now into the product data")
    logger.info("now starting")
    # logger.info(f"{i+1}/{len(response_data['products'])}")
    # Name
    title = product_data["name"]

    # Manufacturer
    sku = product_data.get("sku", None)

    # UPC
    upc = product_data.get("upc", None)
    object_id = product_data.get("id")

    availability_status = product_data.get("status", None)
    availability = 1 if availability_status == "Available" else 0

    # Image URL
    imagePath = product_data["imagePath"]

    image = f"https://media.mwstatic.com/product-images/src/Primary/{imagePath}"
    images = [
        "https://media.mwstatic.com/product-images/src/Primary/" + img["path"]
        for img in response_data["productFamily"]["images"]
    ]
    images.append(image)

    # Product URL

    product_url = f"https://www.midwayusa.com/product/{family_id}?pid={object_id}"

    # Prices
    price_view_data = product_data.get("priceViewModel", None)
    marked_price, selling_price, discount_price = get_price(price_view_data)

    product_dict = {
        "features": features,
        "availability": availability,
        "description": description,
        "category": category,
        "title": title,
        "brand": brand_name,
        "sku": sku,
        "upc": upc,
        "images": images,
        "product_url": product_url,
        "price": marked_price,
        "sale_price": selling_price,
        # "discount_price": discount_price,
        # "caliber": None,
        # "model": None,
    }
    products.append(product_dict)
    return products
