import re
import logging

logger = logging.getLogger()


def get_subcategory_info(soup):
    logger.info("Getting sub category information.")

    # Find the wrapper element containing subcategory information
    subcat_element_wrapper = soup.find("div", {"class": "subcat-row"})

    # Find all subcategory elements within the wrapper
    subcat_elements = subcat_element_wrapper.find_all("a", {"class": "subcat-column"})
    data_list = []

    # Loop through each subcategory element
    for subcat_element in subcat_elements:
        # Extract link and name from the subcategory element
        link = subcat_element.get("href")
        name = subcat_element.text.strip()

        # Find the image element within the subcategory element
        img_element = subcat_element.find("div", {"class": "subcat-img"})

        try:
            # Extract the style attribute value from the image element
            style_attribute = re.search(r'style="(.*?)"', str(img_element)).group(1)

            # Extract the image URL from the style attribute using a regular expression
            image_url = re.search(
                r"background-image: url\('(.*?)'\)", style_attribute
            ).group(1)
        except Exception as e:
            # Handle the case where the image URL extraction fails
            logger.warning(f"Failed to extract image URL: {str(e)}")
            image_url = None

        # Create a dictionary with subcategory information and add it to the data list
        data = {
            "name": name,
            "url": link,
            "img_url": image_url,
        }
        data_list.append(data)
    # Logging: Log the obtained data
    logger.info(f"sub category data obtained: {data_list}")

    # Logging: Log before returning the data
    logger.info(f"Returning product sub category data.")

    # Return the list of subcategory information dictionaries
    return data_list


def get_product_breadcrumb(breadcrumb_element):
    # Logging: Start of the function
    logger.info("Getting product breadcrumb information.")

    # Find the unordered list containing breadcrumb items
    breadcrumb_items = breadcrumb_element.find("ul", {"class": "items"})

    # Logging: Check if the breadcrumb_items is found
    if not breadcrumb_items:
        logger.warning("No breadcrumb items found.")
        return {}

    # Find all list items within the unordered list
    li_elements = breadcrumb_items.find_all("li", {"class": "item"})
    data = {}
    last_level = 0

    # Loop through each list item
    for i, li in enumerate(li_elements):
        # Find the anchor tag within the list item
        a_tag = li.find("a")

        if a_tag:
            # Extract name and link from the anchor tag
            name = a_tag.text.strip()
            link = a_tag.get("href")
            key = f"level_{i}"
            last_level = i
        else:
            # Use the last class name as the key or create a default key
            key = li.get("class")[-1] if li.get("class") else f"level_{i}"
            name = li.text.strip()
            link = None

        # Create a dictionary entry for the current breadcrumb level
        data[key] = {"name": name, "url": link}

    try:
        # Add a "category" key to the dictionary with the name of the last breadcrumb level
        data["category"] = data[f"level_{last_level}"]["name"]
    except Exception as e:
        logger.error(f"Error obtaining product category: {e}")
        logger.info(f"Adding default value to category..")
        data["category"] = "guns"

    # Logging: Log the obtained data
    logger.info(f"Breadcrumb data obtained: {data}")

    # Logging: Log before returning the data
    logger.info(f"Returning product breadcrumb data.")

    # Return the dictionary representing the breadcrumb hierarchy
    return data


def get_product_pricing(element):
    # Logging: Start of the function
    logger.info("Getting product pricing information.")

    # Find the current price element
    current_price_element = element.find("span", {"data-price-type": "finalPrice"})
    old_price_element = element.find("span", {"data-price-type": "oldPrice"})

    # Logging: Check if the current price element is found
    if not current_price_element:
        logger.warning("No current price element found.")

    # Logging: Check if the old price element is found
    if not old_price_element:
        logger.warning("No old price element found.")

    # Extract current and old prices from the elements
    try:
        current_price = current_price_element.get("data-price-amount")
    except:
        current_price = (
            current_price_element.text.strip().replace("$", "").replace(",", "")
            if current_price_element
            else None
        )

    try:
        old_price = old_price_element.get("data-price-amount")
    except:
        old_price = (
            old_price_element.text.strip().replace("$", "").replace(",", "")
            if old_price_element
            else None
        )

    # Create a dictionary with pricing information
    data = {
        "offer_price": float(current_price) if current_price else None,
        "list_price": float(old_price) if old_price else None,
        "save_price": None,
    }

    # Logging: Log the obtained data
    logger.info(f"Pricing data obtained: {data}")

    # Logging: Log before returning the data
    logger.info("Returning product pricing data.")

    # Return the dictionary representing the pricing information
    return data


def get_product_attributes(element):
    # Logging: Start of the function
    logger.info("Getting product attributes information.")

    # Find all table rows within the element
    trs = element.find_all("tr")
    data = {}

    # Loop through each table row
    for tr in trs:
        # Find the header and content elements within the table row
        header = tr.find("th").text.strip() if tr.find("th") else ""
        content = tr.find("td").text.strip() if tr.find("td") else ""

        # Add the header and content to the data dictionary
        data[header] = content

    # Logging: Log the obtained data
    logger.info(f"Attributes data obtained: {data}")

    # Logging: Log before returning the data
    logger.info("Returning product attributes data.")

    # Return the dictionary representing the product attributes
    return data


def get_product_media(element):
    # Logging: Start of the function
    logger.info("Getting product media information.")

    # Find all image elements within the element
    imgs = element.find_all("img")
    data = []

    # Loop through each image element
    for i, img in enumerate(imgs):
        # Check if the image element is present
        if img:
            # Append the image source URL to the data list
            data.append(img.get("src"))

    # Logging: Log the obtained data
    logger.info(f"Media data obtained: {data}")

    # Logging: Log before returning the data
    logger.info("Returning product media data.")

    # Return the list of image source URLs
    return data


def is_next_page_available(soup):
    logger.info("Checking for next page.")
    logger.info("Searching for link in the header")

    next_url = None
    head = soup.find("head")
    next_element = head.find("link", {"rel": "next"})
    if next_element:
        next_url = next_element.get("href")
        logger.info(f"Next Page is available: {next_url}")
    else:
        logger.warning(f"Next Page not found,")

    # previous_element = head.find('link', {'rel': 'prev'})
    # previous_url = previous_element.get('href') if previous_element else None

    return bool(next_url), next_url


def is_next_button_available(soup):
    # Logging: Start of the function
    logger.info("Checking if the next button is available.")

    # Find the pagination element
    pagination_element = soup.find("div", {"class": "pages"})

    # Logging: Check if the pagination element is found
    if not pagination_element:
        logger.warning("No pagination element found.")
        return False, None

    # Find the page action element within the pagination element
    page_action_element = pagination_element.find("div", {"class": "pages__actions"})

    # Find the "Previous" and "Next" elements within the page action element
    previous_element = page_action_element.find("a", {"title": "Previous"})
    next_element = page_action_element.find("a", {"title": "Next"})

    # Check if the "Next" element is present and not disabled
    if next_element and not next_element.has_attr("disabled"):
        next_url = next_element.get("href")
        # Logging: Log the obtained data
        logger.info(f"Next button is available. {next_url}")

        # Logging: Log before returning the data
        logger.info("Returning True and next button URL.")
        return True, next_url

    # Logging: Log before returning the data
    logger.info("Next button is not available.")
    return False, None


def get_products_urls(soup):
    # Logging: Start of the function
    logger.info("Getting product URLs.")

    # Find the product list wrapper
    product_list_wrapper = soup.find("ol", {"class": "product-items"})

    # Logging: Check if the product list wrapper is found
    if not product_list_wrapper:
        logger.warning("No product list wrapper found.")
        return []

    # Find all product items within the product list wrapper
    product_items = product_list_wrapper.find_all("li")
    url_list = []

    # Loop through each product item
    for products in product_items:
        # Find the anchor tag within the product item
        a_tag = products.find("a")

        # Check if the anchor tag is present
        if a_tag:
            # Extract the URL from the anchor tag
            url = a_tag.get("href")

            # Check if the URL is present
            if url:
                # Append the URL to the list
                url_list.append(url)

    # Logging: Log the obtained data
    logger.info(f"Product URLs obtained: {url_list}")

    # Logging: Log before returning the data
    logger.info("Returning product URLs.")

    # Return the list of product URLs
    return url_list


def get_features_data(element):
    # Logging: Start of the function
    logger.info("Getting features element information.")

    # Find the first child element of the input element
    features_child_element = element.findChild()

    # Logging: Check if a child element is found
    if not features_child_element:
        logger.warning("No child element found.")
        return {}

    # Get the name of the child element
    child_element = features_child_element.name
    data_dict = {}
    data_list = []

    # Check the type of child element
    if child_element == "ul":
        for ul in element.find_all("ul"):
            # Find all list items within the unordered list
            li_tags = features_child_element.find_all("li")
            for i, tags in enumerate(li_tags):
                # Find the first child element within the list item
                tags_child = tags.findChild()

                # Check if a child element is found
                if tags_child:
                    # Check if the child element is a "strong" tag
                    if tags_child.name == "strong":
                        strong_tag = tags.find("strong")
                        key = strong_tag.text.strip().rstrip(":").replace("\xa0", " ")
                        if strong_tag.nextSibling:
                            value = strong_tag.nextSibling.text.strip().replace(
                                "\xa0", " "
                            )
                        else:
                            value = key
                            key = "details"
                        data_dict[key.lower()] = value
                    else:
                        # If not a "strong" tag, treat it as details
                        data_dict["details"] = tags_child.text.strip().replace(
                            "\xa0", " "
                        )
                else:
                    # If no child element found, append the text to the data list
                    data_list.append(tags.text.strip().replace("\xa0", " "))

    elif child_element == "p":
        p_tags = element.find_all("p")
        for p_tag in p_tags:
            # Find the first child element within the paragraph
            # tags_child = features_child_element.findChild()
            tags_child = p_tag.findChild()

            # Check if a child element is found
            if tags_child:
                # Check if the child element is a "strong" tag
                if tags_child.name == "strong":
                    for strong_tag in p_tag.find_all("strong"):
                        key = strong_tag.text.strip().rstrip(":").replace("\xa0", " ")
                        if strong_tag.nextSibling:
                            value = strong_tag.nextSibling.text.strip().replace(
                                "\xa0", " "
                            )
                        else:
                            value = key
                            key = "details"
                        data_dict[key.lower()] = value
                else:
                    # If not a "strong" tag, treat it as details
                    data_dict["details"] = p_tag.text.strip().replace("\xa0", " ")
            else:
                # If no child element found, append the text to the data list
                data_list.append(p_tag.text.strip().replace("\xa0", " "))

    # Check if there is any data in the list, and if so, add it to the data_dict
    if data_list:
        data_dict["details"] = data_list

    # Logging: Log the obtained data
    logger.info(f"Features element data obtained: {data_dict}")

    # Logging: Log before returning the data
    logger.info("Returning features element data.")

    # Return the dictionary representing the features element information
    return data_dict


def get_description_element(soup):
    value_list = [{"id": "description"}, {"id": "product-set-tab"}]
    for value in value_list:
        element = soup.find("div", value)
        if element:
            return element


def get_features_element(description):
    logger.info("Getting Features element..")
    element = None
    value_list = [
        {"class": "value", "itemprop": "description"},
        {"class": "value", "itemprop": "sort_description"},
    ]
    for value in value_list:
        element = description.find("div", value)
        if element:
            return element
    logger.warning("element not found using itemprop: descrption and sort_description")
    if not element:
        value = {"class": "product attribute overview"}
        overview = description.find("div", value)
        if overview:
            element = overview.find("div", {"class": "value"})
            logger.info("Feature of the product found. ")

        else:
            logger.warning("Feature of the product not found. Check it again ")
    return element


def get_long_description(description_element):
    element_list = []
    a = description_element.find("div", {"class": "product attribute description"})
    if a:
        details = a.find("div", {"class": "value"})
        if details:
            element_filter = [
                element
                for element in details.find_all("div")
                if element.has_attr("data-content-type")
            ]
            if element_filter:
                element_filter = element_filter[0]
                for element in element_filter.findChildren():
                    if element.has_attr("data-role"):
                        break
                    element_list.append(element.text.strip())
            return "\n".join(element_list)
    return None


def get_product_data(soup):
    # Logging: Start of the function
    logger.info("Getting product data.")

    # breadcrumb information
    breadcrumb_element = soup.find("div", {"class": "breadcrumbs"})
    if breadcrumb_element:
        breadcrumb_data = get_product_breadcrumb(breadcrumb_element)
    else:
        breadcrumb_data = {"category": None}

    # stock information
    stock_element = soup.find("div", {"class": "product-info-stock-sku"})
    logger.debug(f"Stock element: {stock_element}")

    stock_data = stock_element.text.strip() if stock_element else None
    logger.info(f"Stock data: {stock_data}")
    availability = 1 if stock_data == "In stock" else 0

    # title information
    title_element = soup.find("h1", {"class": "page-title"})
    logger.debug(f"Title element: {title_element}")
    title_data = title_element.text.strip() if title_element else None
    logger.info(f"Title data: {title_data}")

    # pricing information
    pricing_element = soup.find("div", {"class": "product-info-price"})
    logger.debug(f"Price element: {pricing_element}")

    if pricing_element:
        pricing_data = get_product_pricing(pricing_element)
    else:
        pricing_data = {
            "offer_price": None,
            "list_price": None,
            "save_price": None,
        }
    logger.info(f"Price data: {pricing_data}")

    # attribute information
    attributes_element = soup.find("table", {"id": "product-attribute-specs-table"})
    logger.debug(f"Attributes element: {attributes_element}")

    if attributes_element:
        attributes_data = get_product_attributes(attributes_element)
    else:
        attributes_data = {}
    logger.info(f"Attributes data: {pricing_data}")

    # media information
    media_element = soup.find("div", {"class": "product media"})
    logger.debug(f"Media Element: {media_element}")
    if media_element:
        images_data = get_product_media(media_element)
    else:
        images_data = []
    logger.info(f"Media data: {images_data}")

    # description information
    description_element = get_description_element(soup)
    if description_element:
        features_element = get_features_element(description_element)
        logger.debug(f"features element: {features_element}")

        if features_element:
            # features_data = features_element.text.strip() if features_element else None
            features_data = get_features_data(features_element)
        else:
            features_data = {}
        logger.info(f"features data: {features_data}")

        long_description_data = get_long_description(description_element)
        logger.info(f"Long description data: {long_description_data}")
    else:
        logger.info("Description element not found")
        long_description_data = None
        features_data = {}

    # create dictionary
    product_details = {
        "breadcrumb": breadcrumb_data,
        "images": images_data,
        "title": title_data,
        "pricing": pricing_data,
        "attributes": attributes_data,
        "brand_logo": None,
        "long_description": long_description_data,
        "variation": None,
        "availability": availability,
        "features": features_data,
    }

    # Logging: Log the obtained data
    logger.info(f"Product data obtained: {product_details}")

    # Logging: Log before returning the data
    logger.info("Returning product data.")

    return product_details


def check404(soup):
    content_404 = soup.find("main", {"id": "maincontent"}).find(
        "div", {"class": "noRoute-container"}
    )
    if content_404:
        return True
    return False
