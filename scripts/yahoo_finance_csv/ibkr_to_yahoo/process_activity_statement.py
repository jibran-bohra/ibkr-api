import csv
import pandas as pd
import re
import os
from collections import defaultdict


def sanitize_filename(name):
    """Converts a statement name into a valid filename."""
    s = name.replace(" ", "_").replace("/", "_")
    s = re.sub(r"[^a-zA-Z0-9_]", "", s)
    return s.lower()


def process_activity_statement(filepath, output_dir):
    """
    Parses an IBKR activity statement CSV and saves each section into a separate CSV file.
    """
    tables = []
    current_statement = None
    current_header = None
    current_data = []

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row or len(row) < 2:
                    continue

                statement_name, row_type, *content = row

                if row_type == "Header":
                    if current_statement and current_header and current_data:
                        tables.append(
                            {
                                "name": current_statement,
                                "header": current_header,
                                "data": current_data,
                            }
                        )

                    current_statement = statement_name
                    current_header = content
                    current_data = []
                elif row_type == "Data" and current_header:
                    if statement_name == current_statement:
                        # Pad data row to match header length if necessary
                        num_missing_cols = len(current_header) - len(content)
                        if num_missing_cols > 0:
                            content.extend([""] * num_missing_cols)
                        elif num_missing_cols < 0:
                            content = content[: len(current_header)]
                        current_data.append(content)

        if current_statement and current_header and current_data:
            tables.append(
                {
                    "name": current_statement,
                    "header": current_header,
                    "data": current_data,
                }
            )

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        statement_counts = defaultdict(int)
        for table in tables:
            statement_counts[table["name"]] += 1

        table_save_counts = defaultdict(int)
        for table in tables:
            name = table["name"]
            header = table["header"]
            data = table["data"]

            # Some headers might be empty, let's provide default names.
            header = [f"col_{i + 1}" if not h else h for i, h in enumerate(header)]

            df = pd.DataFrame(data, columns=header)

            sanitized_name = sanitize_filename(name)

            count = statement_counts[name]
            table_save_counts[name] += 1

            if count > 1:
                filename = f"{sanitized_name}_{table_save_counts[name]}.csv"
            else:
                filename = f"{sanitized_name}.csv"

            output_path = os.path.join(output_dir, filename)
            df.to_csv(output_path, index=False)
            print(f"Saved '{output_path}'")
    except FileNotFoundError:
        print(f"Error: The file '{filepath}' was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    # Assuming the script is in scripts/yahoo_finance_csv/
    # and the data file is in scripts/yahoo_finance_csv/data/
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(script_dir, "data")
        input_filepath = os.path.join(data_dir, "activity_statement.csv")
        output_dir_path = os.path.join(data_dir, "activity_statement_parts")

        process_activity_statement(input_filepath, output_dir_path)
    except NameError:
        # Handle case where __file__ is not defined (e.g., in an interactive environment)
        print(
            "Could not determine script directory. Please run this script from a file."
        )
