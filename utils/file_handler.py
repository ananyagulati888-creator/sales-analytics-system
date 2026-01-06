def read_sales_file(file_path):
    with open(file_path, "r", encoding="latin-1") as file:
        return file.readlines()

