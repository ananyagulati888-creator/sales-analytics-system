# API handling will be implemented in future
import requests

def fetch_all_products():
    """
    Fetches all products from DummyJSON API

    Returns: list of product dictionaries
    """

    url = "https://dummyjson.com/products?limit=100"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        data = response.json()
        products = data.get("products", [])

        print(f"✅ Successfully fetched {len(products)} products from API")
        return products

    except requests.exceptions.RequestException as e:
        print("❌ Failed to fetch products from API")
        print("Error:", e)
        return []


def create_product_mapping(api_products):
    """
    Creates a mapping of product IDs to product info

    Returns: dictionary mapping product IDs to info
    """

    product_mapping = {}

    for product in api_products:
        product_id = product.get("id")

        if product_id is not None:
            product_mapping[product_id] = {
                "title": product.get("title"),
                "category": product.get("category"),
                "brand": product.get("brand"),
                "rating": product.get("rating")
            }

    return product_mapping



def enrich_sales_data(transactions, product_mapping):
    """
    Enriches transaction data with API product information

    Returns: list of enriched transaction dictionaries
    """

    enriched_transactions = []

    for tx in transactions:
        enriched_tx = tx.copy()

        try:
            # Extract numeric ID from ProductID (P101 -> 101)
            product_id = int(tx["ProductID"][1:])

            if product_id in product_mapping:
                api_product = product_mapping[product_id]

                enriched_tx["API_Category"] = api_product.get("category")
                enriched_tx["API_Brand"] = api_product.get("brand")
                enriched_tx["API_Rating"] = api_product.get("rating")
                enriched_tx["API_Match"] = True
            else:
                enriched_tx["API_Category"] = None
                enriched_tx["API_Brand"] = None
                enriched_tx["API_Rating"] = None
                enriched_tx["API_Match"] = False

        except Exception:
            enriched_tx["API_Category"] = None
            enriched_tx["API_Brand"] = None
            enriched_tx["API_Rating"] = None
            enriched_tx["API_Match"] = False

        enriched_transactions.append(enriched_tx)

    return enriched_transactions



def save_enriched_data(enriched_transactions, filename="data/enriched_sales_data.txt"):
    """
    Saves enriched transactions back to file
    """

    header = (
        "TransactionID|Date|ProductID|ProductName|Quantity|UnitPrice|"
        "CustomerID|Region|API_Category|API_Brand|API_Rating|API_Match\n"
    )

    with open(filename, "w", encoding="utf-8") as file:
        file.write(header)

        for tx in enriched_transactions:
            line = (
                f"{tx.get('TransactionID')}|"
                f"{tx.get('Date')}|"
                f"{tx.get('ProductID')}|"
                f"{tx.get('ProductName')}|"
                f"{tx.get('Quantity')}|"
                f"{tx.get('UnitPrice')}|"
                f"{tx.get('CustomerID')}|"
                f"{tx.get('Region')}|"
                f"{tx.get('API_Category')}|"
                f"{tx.get('API_Brand')}|"
                f"{tx.get('API_Rating')}|"
                f"{tx.get('API_Match')}\n"
            )

            file.write(line)
