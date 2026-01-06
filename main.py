from utils.file_handler import read_sales_file
from utils.data_processor import clean_sales_data

def main():
    lines = read_sales_file("data/sales_data.txt")
    clean_sales_data(lines)

if __name__ == "__main__":
    main()
