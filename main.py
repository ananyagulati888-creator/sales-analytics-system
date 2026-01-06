from utils.file_handler import read_sales_data
from utils.data_processor import (
    parse_transactions,
    validate_and_filter
)
from utils.api_handler import (
    fetch_all_products,
    create_product_mapping,
    enrich_sales_data,
    save_enriched_data
)


def main():
    print("🔹 Starting Sales Analytics System")

    # Step 1: Read raw sales data
    raw_lines = read_sales_data("data/sales_data.txt")
    print(f"Read {len(raw_lines)} raw records")

    # Step 2: Parse and clean data
    transactions = parse_transactions(raw_lines)
    print(f"Parsed {len(transactions)} transactions")

    # Step 3: Validate and filter data
    valid_transactions, invalid_count, summary = validate_and_filter(transactions)
    print("Validation Summary:", summary)

    # Step 4: Fetch API product data
    api_products = fetch_all_products()

    # Step 5: Create product mapping
    product_mapping = create_product_mapping(api_products)

    # Step 6: Enrich sales data
    enriched_transactions = enrich_sales_data(valid_transactions, product_mapping)
    print(f"Enriched {len(enriched_transactions)} transactions")

    # Step 7: Save enriched data
    save_enriched_data(enriched_transactions)
    print("✅ Enriched sales data saved to data/enriched_sales_data.txt")


if __name__ == "__main__":
    main()
