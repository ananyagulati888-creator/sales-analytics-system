def read_sales_file(file_path):
    with open(file_path, "r", encoding="latin-1") as file:
        return file.readlines()

def read_sales_data(filename):
    """
    Reads sales data from file handling encoding issues

    Returns: list of raw lines (strings)

    Expected output format:
    ['T001|2024-12-01|P101|Laptop|2|45000|C001|North', ...]
    """

    encodings = ["utf-8", "latin-1", "cp1252"]

    for encoding in encodings:
        try:
            with open(filename, "r", encoding=encoding) as file:
                lines = file.readlines()

                cleaned_lines = []
                for line in lines[1:]:  # skip header
                    line = line.strip()
                    if line:
                        cleaned_lines.append(line)

                return cleaned_lines

        except UnicodeDecodeError:
            continue

        except FileNotFoundError:
            print(f"Error: File '{filename}' not found.")
            return []

    print("Error: Unable to read file due to encoding issues.")
    return []
