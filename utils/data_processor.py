def parse_transactions(raw_lines):
    """
    Parses raw lines into clean list of dictionaries

    Returns: list of dictionaries with keys:
    ['TransactionID', 'Date', 'ProductID', 'ProductName',
     'Quantity', 'UnitPrice', 'CustomerID', 'Region']
    """

    cleaned_transactions = []

    for line in raw_lines:
        parts = line.split("|")

        # Skip rows with incorrect number of fields
        if len(parts) != 8:
            continue

        transaction_id, date, product_id, product_name, quantity, unit_price, customer_id, region = parts

        # Handle commas in ProductName
        product_name = product_name.replace(",", "")

        # Handle commas in numeric fields and convert types
        try:
            quantity = int(quantity)
            unit_price = float(unit_price.replace(",", ""))
        except ValueError:
            continue

        transaction = {
            "TransactionID": transaction_id,
            "Date": date,
            "ProductID": product_id,
            "ProductName": product_name,
            "Quantity": quantity,
            "UnitPrice": unit_price,
            "CustomerID": customer_id,
            "Region": region
        }

        cleaned_transactions.append(transaction)

    return cleaned_transactions


def validate_and_filter(transactions, region=None, min_amount=None, max_amount=None):
    """
    Validates transactions and applies optional filters
    """

    valid_transactions = []
    invalid_count = 0

    total_input = len(transactions)
    filtered_by_region = 0
    filtered_by_amount = 0

    # collect available regions and amount range
    regions = set()
    amounts = []

    for tx in transactions:
        regions.add(tx.get("Region"))
        amounts.append(tx.get("Quantity", 0) * tx.get("UnitPrice", 0))

    if regions:
        print("Available regions:", sorted(regions))
    if amounts:
        print("Transaction amount range:", min(amounts), "to", max(amounts))

    for tx in transactions:
        # Validation rules
        if (
            tx.get("Quantity", 0) <= 0 or
            tx.get("UnitPrice", 0) <= 0 or
            not tx.get("TransactionID", "").startswith("T") or
            not tx.get("ProductID", "").startswith("P") or
            not tx.get("CustomerID", "").startswith("C")
        ):
            invalid_count += 1
            continue

        amount = tx["Quantity"] * tx["UnitPrice"]

        # Filter by region
        if region and tx["Region"] != region:
            filtered_by_region += 1
            continue

        # Filter by amount
        if min_amount and amount < min_amount:
            filtered_by_amount += 1
            continue
        if max_amount and amount > max_amount:
            filtered_by_amount += 1
            continue

        valid_transactions.append(tx)

    filter_summary = {
        "total_input": total_input,
        "invalid": invalid_count,
        "filtered_by_region": filtered_by_region,
        "filtered_by_amount": filtered_by_amount,
        "final_count": len(valid_transactions)
    }

    return valid_transactions, invalid_count, filter_summary


def calculate_total_revenue(transactions):
    """
    Calculates total revenue from all transactions

    Returns: float (total revenue)
    """

    total_revenue = 0.0

    for tx in transactions:
        total_revenue += tx["Quantity"] * tx["UnitPrice"]

    return total_revenue


def region_wise_sales(transactions):
    """
    Analyzes sales by region

    Returns: dictionary with region statistics
    """

    region_stats = {}
    total_sales_all = 0.0

    # First pass: calculate total sales per region
    for tx in transactions:
        region = tx["Region"]
        sale_amount = tx["Quantity"] * tx["UnitPrice"]
        total_sales_all += sale_amount

        if region not in region_stats:
            region_stats[region] = {
                "total_sales": 0.0,
                "transaction_count": 0
            }

        region_stats[region]["total_sales"] += sale_amount
        region_stats[region]["transaction_count"] += 1

    # Second pass: calculate percentage of total sales
    for region in region_stats:
        region_stats[region]["percentage"] = round(
            (region_stats[region]["total_sales"] / total_sales_all) * 100, 2
        )

    # Sort regions by total_sales descending
    sorted_regions = dict(
        sorted(
            region_stats.items(),
            key=lambda item: item[1]["total_sales"],
            reverse=True
        )
    )

    return sorted_regions



def top_selling_products(transactions, n=5):
    """
    Finds top n products by total quantity sold

    Returns: list of tuples
    (ProductName, TotalQuantity, TotalRevenue)
    """

    product_summary = {}

    # Aggregate by ProductName
    for tx in transactions:
        product = tx["ProductName"]
        quantity = tx["Quantity"]
        revenue = quantity * tx["UnitPrice"]

        if product not in product_summary:
            product_summary[product] = {
                "total_quantity": 0,
                "total_revenue": 0.0
            }

        product_summary[product]["total_quantity"] += quantity
        product_summary[product]["total_revenue"] += revenue

    # Convert to list of tuples
    product_list = [
        (product,
         data["total_quantity"],
         round(data["total_revenue"], 2))
        for product, data in product_summary.items()
    ]

    # Sort by total quantity descending
    product_list.sort(key=lambda x: x[1], reverse=True)

    # Return top n products
    return product_list[:n]



def customer_analysis(transactions):
    """
    Analyzes customer purchase patterns

    Returns: dictionary of customer statistics
    """

    customer_stats = {}

    # Aggregate data per customer
    for tx in transactions:
        customer_id = tx["CustomerID"]
        product = tx["ProductName"]
        amount = tx["Quantity"] * tx["UnitPrice"]

        if customer_id not in customer_stats:
            customer_stats[customer_id] = {
                "total_spent": 0.0,
                "purchase_count": 0,
                "products_bought": set()
            }

        customer_stats[customer_id]["total_spent"] += amount
        customer_stats[customer_id]["purchase_count"] += 1
        customer_stats[customer_id]["products_bought"].add(product)

    # Calculate average order value
    for customer_id in customer_stats:
        total = customer_stats[customer_id]["total_spent"]
        count = customer_stats[customer_id]["purchase_count"]

        customer_stats[customer_id]["avg_order_value"] = round(total / count, 2)

        # Convert set to list for output
        customer_stats[customer_id]["products_bought"] = list(
            customer_stats[customer_id]["products_bought"]
        )

    # Sort customers by total_spent descending
    sorted_customers = dict(
        sorted(
            customer_stats.items(),
            key=lambda item: item[1]["total_spent"],
            reverse=True
        )
    )

    return sorted_customers


def daily_sales_trend(transactions):
    """
    Analyzes sales trends by date

    Returns: dictionary sorted by date
    """

    daily_stats = {}

    for tx in transactions:
        date = tx["Date"]
        amount = tx["Quantity"] * tx["UnitPrice"]

        if date not in daily_stats:
            daily_stats[date] = {
                "revenue": 0.0,
                "transaction_count": 0,
                "customers": set()
            }

        daily_stats[date]["revenue"] += amount
        daily_stats[date]["transaction_count"] += 1
        daily_stats[date]["customers"].add(tx["CustomerID"])

    # Convert customer sets to counts
    for date in daily_stats:
        daily_stats[date]["unique_customers"] = len(daily_stats[date]["customers"])
        del daily_stats[date]["customers"]

    # Sort chronologically by date
    sorted_daily_stats = dict(sorted(daily_stats.items()))

    return sorted_daily_stats


def find_peak_sales_day(transactions):
    """
    Identifies the date with highest revenue

    Returns: tuple (date, revenue, transaction_count)
    """

    daily_revenue = {}

    for tx in transactions:
        date = tx["Date"]
        amount = tx["Quantity"] * tx["UnitPrice"]

        if date not in daily_revenue:
            daily_revenue[date] = {
                "revenue": 0.0,
                "transaction_count": 0
            }

        daily_revenue[date]["revenue"] += amount
        daily_revenue[date]["transaction_count"] += 1

    peak_date = None
    peak_revenue = 0.0
    peak_count = 0

    for date, stats in daily_revenue.items():
        if stats["revenue"] > peak_revenue:
            peak_revenue = stats["revenue"]
            peak_count = stats["transaction_count"]
            peak_date = date

    return peak_date, peak_revenue, peak_count


def low_performing_products(transactions, threshold=10):
    """
    Identifies products with low sales

    Returns: list of tuples
    (ProductName, TotalQuantity, TotalRevenue)
    """

    product_summary = {}

    # Aggregate data by product
    for tx in transactions:
        product = tx["ProductName"]
        quantity = tx["Quantity"]
        revenue = quantity * tx["UnitPrice"]

        if product not in product_summary:
            product_summary[product] = {
                "total_quantity": 0,
                "total_revenue": 0.0
            }

        product_summary[product]["total_quantity"] += quantity
        product_summary[product]["total_revenue"] += revenue

    # Filter products below threshold
    low_products = [
        (product,
         data["total_quantity"],
         round(data["total_revenue"], 2))
        for product, data in product_summary.items()
        if data["total_quantity"] < threshold
    ]

    # Sort by TotalQuantity ascending
    low_products.sort(key=lambda x: x[1])

    return low_products



def high_performing_products(transactions, threshold=50):
    """
    Identifies products with high sales performance

    Returns: list of tuples
    (ProductName, TotalQuantity, TotalRevenue)
    """

    product_summary = {}

    for tx in transactions:
        product = tx["ProductName"]
        quantity = tx["Quantity"]
        revenue = quantity * tx["UnitPrice"]

        if product not in product_summary:
            product_summary[product] = {
                "total_quantity": 0,
                "total_revenue": 0.0
            }

        product_summary[product]["total_quantity"] += quantity
        product_summary[product]["total_revenue"] += revenue

    high_products = [
        (product,
         data["total_quantity"],
         round(data["total_revenue"], 2))
        for product, data in product_summary.items()
        if data["total_quantity"] >= threshold
    ]

    # Sort by quantity descending
    high_products.sort(key=lambda x: x[1], reverse=True)

    return high_products



from datetime import datetime
from collections import defaultdict


def generate_sales_report(transactions, enriched_transactions, output_file="output/sales_report.txt"):
    """
    Generates a comprehensive sales analytics text report
    """

    with open(output_file, "w", encoding="utf-8") as file:

        # ---------------- HEADER ----------------
        file.write("SALES ANALYTICS REPORT\n")
        file.write("=" * 50 + "\n")
        file.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        file.write(f"Records Processed: {len(transactions)}\n\n")

        # ---------------- OVERALL SUMMARY ----------------
        total_revenue = sum(tx["Quantity"] * tx["UnitPrice"] for tx in transactions)
        avg_order_value = total_revenue / len(transactions) if transactions else 0
        dates = sorted(tx["Date"] for tx in transactions)

        file.write("OVERALL SUMMARY\n")
        file.write("-" * 50 + "\n")
        file.write(f"Total Revenue: {total_revenue:,.2f}\n")
        file.write(f"Total Transactions: {len(transactions)}\n")
        file.write(f"Average Order Value: {avg_order_value:,.2f}\n")
        if dates:
            file.write(f"Date Range: {dates[0]} to {dates[-1]}\n\n")

        # ---------------- REGION-WISE PERFORMANCE ----------------
        region_stats = defaultdict(lambda: {"sales": 0, "count": 0})

        for tx in transactions:
            amount = tx["Quantity"] * tx["UnitPrice"]
            region_stats[tx["Region"]]["sales"] += amount
            region_stats[tx["Region"]]["count"] += 1

        file.write("REGION-WISE PERFORMANCE\n")
        file.write("-" * 50 + "\n")
        for region, stats in sorted(region_stats.items(), key=lambda x: x[1]["sales"], reverse=True):
            percent = (stats["sales"] / total_revenue) * 100 if total_revenue else 0
            file.write(
                f"{region}: Sales={stats['sales']:,.2f}, "
                f"% of Total={percent:.2f}%, "
                f"Transactions={stats['count']}\n"
            )
        file.write("\n")

        # ---------------- TOP PRODUCTS ----------------
        product_summary = defaultdict(lambda: {"qty": 0, "rev": 0})

        for tx in transactions:
            product_summary[tx["ProductName"]]["qty"] += tx["Quantity"]
            product_summary[tx["ProductName"]]["rev"] += tx["Quantity"] * tx["UnitPrice"]

        file.write("TOP PRODUCTS\n")
        file.write("-" * 50 + "\n")
        for i, (product, stats) in enumerate(
            sorted(product_summary.items(), key=lambda x: x[1]["qty"], reverse=True)[:5], start=1
        ):
            file.write(
                f"{i}. {product} | Quantity={stats['qty']} | Revenue={stats['rev']:,.2f}\n"
            )
        file.write("\n")

        # ---------------- TOP CUSTOMERS ----------------
        customer_summary = defaultdict(lambda: {"spent": 0, "orders": 0})

        for tx in transactions:
            customer_summary[tx["CustomerID"]]["spent"] += tx["Quantity"] * tx["UnitPrice"]
            customer_summary[tx["CustomerID"]]["orders"] += 1

        file.write("TOP CUSTOMERS\n")
        file.write("-" * 50 + "\n")
        for i, (cust, stats) in enumerate(
            sorted(customer_summary.items(), key=lambda x: x[1]["spent"], reverse=True)[:5], start=1
        ):
            avg = stats["spent"] / stats["orders"]
            file.write(
                f"{i}. {cust} | Total Spent={stats['spent']:,.2f} | Avg Order={avg:,.2f}\n"
            )
        file.write("\n")

        # ---------------- DAILY SALES TREND ----------------
        daily_stats = defaultdict(lambda: {"rev": 0, "count": 0, "customers": set()})

        for tx in transactions:
            daily_stats[tx["Date"]]["rev"] += tx["Quantity"] * tx["UnitPrice"]
            daily_stats[tx["Date"]]["count"] += 1
            daily_stats[tx["Date"]]["customers"].add(tx["CustomerID"])

        file.write("DAILY SALES TREND\n")
        file.write("-" * 50 + "\n")
        for date in sorted(daily_stats):
            file.write(
                f"{date}: Revenue={daily_stats[date]['rev']:,.2f}, "
                f"Transactions={daily_stats[date]['count']}, "
                f"Unique Customers={len(daily_stats[date]['customers'])}\n"
            )
        file.write("\n")

        # ---------------- API ENRICHMENT SUMMARY ----------------
        enriched = sum(1 for tx in enriched_transactions if tx.get("API_Match"))
        not_enriched = len(enriched_transactions) - enriched

        file.write("API ENRICHMENT SUMMARY\n")
        file.write("-" * 50 + "\n")
        file.write(f"Total Records Enriched: {enriched}\n")
        file.write(f"Records Not Enriched: {not_enriched}\n")

    print(f"✅ Sales report generated at {output_file}")



from datetime import datetime
from collections import defaultdict

def generate_sales_report(transactions, enriched_transactions, output_file="output/sales_report.txt"):
    """
    Generates a comprehensive formatted text report and saves it to a file
    """

    # ---------- BASIC METRICS ----------
    total_transactions = len(transactions)
    total_revenue = sum(tx["Quantity"] * tx["UnitPrice"] for tx in transactions)
    avg_order_value = total_revenue / total_transactions if total_transactions else 0

    dates = [tx["Date"] for tx in transactions]
    date_range = f"{min(dates)} to {max(dates)}" if dates else "N/A"

    # ---------- REGION PERFORMANCE ----------
    region_stats = defaultdict(lambda: {"revenue": 0, "count": 0})
    for tx in transactions:
        amount = tx["Quantity"] * tx["UnitPrice"]
        region_stats[tx["Region"]]["revenue"] += amount
        region_stats[tx["Region"]]["count"] += 1

    # ---------- TOP PRODUCTS ----------
    product_stats = defaultdict(lambda: {"qty": 0, "revenue": 0})
    for tx in transactions:
        product = tx["ProductName"]
        qty = tx["Quantity"]
        amount = qty * tx["UnitPrice"]
        product_stats[product]["qty"] += qty
        product_stats[product]["revenue"] += amount

    top_products = sorted(
        product_stats.items(),
        key=lambda x: x[1]["revenue"],
        reverse=True
    )[:5]

    # ---------- TOP CUSTOMERS ----------
    customer_stats = defaultdict(lambda: {"spent": 0, "orders": 0})
    for tx in transactions:
        cid = tx["CustomerID"]
        amount = tx["Quantity"] * tx["UnitPrice"]
        customer_stats[cid]["spent"] += amount
        customer_stats[cid]["orders"] += 1

    top_customers = sorted(
        customer_stats.items(),
        key=lambda x: x[1]["spent"],
        reverse=True
    )[:5]

    # ---------- DAILY SALES ----------
    daily_stats = defaultdict(lambda: {"revenue": 0, "count": 0, "customers": set()})
    for tx in transactions:
        date = tx["Date"]
        amount = tx["Quantity"] * tx["UnitPrice"]
        daily_stats[date]["revenue"] += amount
        daily_stats[date]["count"] += 1
        daily_stats[date]["customers"].add(tx["CustomerID"])

    # ---------- API ENRICHMENT ----------
    enriched_count = sum(1 for tx in enriched_transactions if tx.get("API_Match") is True)
    unenriched = [tx["TransactionID"] for tx in enriched_transactions if not tx.get("API_Match")]

    # ---------- WRITE REPORT ----------
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("SALES ANALYTICS REPORT\n")
        f.write("=" * 60 + "\n")
        f.write(f"Generated on: {datetime.now()}\n")
        f.write(f"Records Processed: {total_transactions}\n\n")

        f.write("OVERALL SUMMARY\n")
        f.write("-" * 60 + "\n")
        f.write(f"Total Revenue: {total_revenue:,.2f}\n")
        f.write(f"Total Transactions: {total_transactions}\n")
        f.write(f"Average Order Value: {avg_order_value:,.2f}\n")
        f.write(f"Date Range: {date_range}\n\n")

        f.write("REGION-WISE PERFORMANCE\n")
        f.write("-" * 60 + "\n")
        for region, data in region_stats.items():
            percent = (data["revenue"] / total_revenue) * 100 if total_revenue else 0
            f.write(f"{region}: Revenue={data['revenue']:,.2f}, "
                    f"Transactions={data['count']}, "
                    f"Percent={percent:.2f}%\n")
        f.write("\n")

        f.write("TOP PRODUCTS\n")
        f.write("-" * 60 + "\n")
        for i, (product, data) in enumerate(top_products, start=1):
            f.write(f"{i}. {product} | Qty={data['qty']} | Revenue={data['revenue']:,.2f}\n")
        f.write("\n")

        f.write("TOP CUSTOMERS\n")
        f.write("-" * 60 + "\n")
        for i, (cid, data) in enumerate(top_customers, start=1):
            f.write(f"{i}. {cid} | Spent={data['spent']:,.2f} | Orders={data['orders']}\n")
        f.write("\n")

        f.write("DAILY SALES TREND\n")
        f.write("-" * 60 + "\n")
        for date in sorted(daily_stats.keys()):
            d = daily_stats[date]
            f.write(f"{date}: Revenue={d['revenue']:,.2f}, "
                    f"Transactions={d['count']}, "
                    f"Unique Customers={len(d['customers'])}\n")
        f.write("\n")

        f.write("API ENRICHMENT SUMMARY\n")
        f.write("-" * 60 + "\n")
        f.write(f"Total Enriched Transactions: {enriched_count}\n")
        f.write(f"Failed Enrichments: {len(unenriched)}\n")
        if unenriched:
            f.write("Unmatched Transaction IDs:\n")
            for tid in unenriched:
                f.write(f"- {tid}\n")

    print(f"Report successfully generated at: {output_file}")
