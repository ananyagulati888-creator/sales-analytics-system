def clean_sales_data(lines):
    total_records = 0
    invalid_records = 0
    valid_records = 0

    for line in lines[1:]:  # skip header
        line = line.strip()
        if line == "":
            continue

        total_records += 1
        parts = line.split("|")

        if len(parts) != 8:
            invalid_records += 1
            continue

        transaction_id, date, product_id, product_name, quantity, unit_price, customer_id, region = parts

        if not transaction_id.startswith("T"):
            invalid_records += 1
            continue

        if customer_id == "" or region == "":
            invalid_records += 1
            continue

        try:
            quantity = int(quantity)
            unit_price = float(unit_price.replace(",", ""))
        except:
            invalid_records += 1
            continue

        if quantity <= 0 or unit_price <= 0:
            invalid_records += 1
            continue

        product_name = product_name.replace(",", "")
        valid_records += 1

    print("Total records parsed:", total_records)
    print("Invalid records removed:", invalid_records)
    print("Valid records after cleaning:", valid_records)
