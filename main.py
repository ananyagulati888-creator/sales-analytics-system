from utils.file_handler import read_sales_data
from utils.data_processor import (
    parse_transactions,
    validate_and_filter,
    generate_sales_report
)
from utils.api_handler import (
    fetch_all_products,
    create_product_mapping,
    enrich_sales_data,
    save_enriched_data
)


def main():
    try:
        print("\n🚀 Starting Sales Analytics System")

        # 1. Read sales data
        raw_lines = read_sales_data("data/sales_data.txt")
        print(f"Read {len(raw_lines)} raw records")

        # 2. Parse data
        transactions = parse_transactions(raw_lines)
        print(f"Parsed {len(transactions)} transactions")

        # 3. Validate & filter (UNPACK PROPERLY)
        valid_transactions, invalid_count, summary = validate_and_filter(transactions)
        print(f"Validation Summary: {summary}")

        # 4. Fetch API products
        api_products = fetch_all_products()
        print(f"Successfully fetched {len(api_products)} products from API")

        product_mapping = create_product_mapping(api_products)

        # 5. Enrich data
        enriched_transactions = enrich_sales_data(valid_transactions, product_mapping)
        print(f"Enriched {len(enriched_transactions)} transactions")

        # 6. Save enriched data
        save_enriched_data(enriched_transactions)
        print("✅ Enriched sales data saved to data/enriched_sales_data.txt")

        # 7. Generate report (THIS WAS MISSING EARLIER)
        generate_sales_report(
            transactions=valid_transactions,
            enriched_transactions=enriched_transactions,
            output_file="output/sales_report.txt"
        )

        print("✅ Report generated at output/sales_report.txt")
        print("\n🎉 PROCESS COMPLETE")

    except Exception as e:
        print("\n❌ ERROR OCCURRED")
        print(str(e))


if __name__ == "__main__":
    main()
