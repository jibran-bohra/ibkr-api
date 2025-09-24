import pandas as pd
import os


def format_trades_for_yahoo(input_filepath, output_filepath):
    """
    Formats the trades CSV file for Yahoo Finance import.
    """
    try:
        # Define the expected columns based on the provided trades.csv structure
        # col_1: DataDiscriminator, col_2: Asset Category, col_3: Currency, col_4: Symbol,
        # col_5: Date/Time, col_6: Quantity, col_7: T. Price, col_8: C. Price,
        # col_9: Proceeds, col_10: Comm/Fee, col_11: Basis, col_12: Realized P/L,
        # col_13: MTM P/L, col_14: Code
        df = pd.read_csv(input_filepath)

        # Remove SubTotal and Total rows
        df = df[df["DataDiscriminator"] == "Order"]

        # Rename columns for Yahoo Finance
        # Yahoo Finance columns: Symbol, Trade Date, Action, Quantity, Price, Commission, Notes
        rename_map = {
            "Symbol": "Symbol",
            "Date/Time": "Trade Date",
            "Quantity": "Quantity",
            "T. Price": "Purchase Price",
            "Comm/Fee": "Commission",
            # "Code": "Comment",
        }

        # Create a new DataFrame with only the required columns
        yahoo_df = df[list(rename_map.keys())].copy()
        yahoo_df.rename(columns=rename_map, inplace=True)

        # Add the 'Action' column
        yahoo_df["Transaction Type"] = None

        # Convert 'Trade Date' to the required format (YYYY-MM-DD)
        yahoo_df["Trade Date"] = pd.to_datetime(yahoo_df["Trade Date"]).dt.strftime(
            "%Y%m%d"
        )

        # Ensure 'Quantity' is positive for both buys and sells
        yahoo_df["Quantity"] = yahoo_df["Quantity"].abs()

        # Ensure 'Commission' is positive
        yahoo_df["Commission"] = yahoo_df["Commission"].abs()

        # Ensure all required columns are present in the DataFrame
        required_columns = [
            "Symbol",
            "Current Price",
            "Date",
            "Time",
            "Change",
            "Open",
            "High",
            "Low",
            "Volume",
            "Trade Date",
            "Purchase Price",
            "Quantity",
            "Commission",
            "High Limit",
            "Low Limit",
            "Comment",
            "Transaction Type",
        ]

        # Add missing columns with default values
        for column in required_columns:
            if column not in yahoo_df.columns:
                if column in [
                    "Current Price",
                    "Change",
                    "Open",
                    "High",
                    "Low",
                    "Volume",
                    "High Limit",
                    "Low Limit",
                ]:
                    yahoo_df[column] = None  # Default numerical value
                else:
                    yahoo_df[column] = ""  # Default string value

        # Reorder columns to match the specified order
        yahoo_df = yahoo_df[required_columns]

        # Save to a new CSV file
        os.makedirs(os.path.dirname(output_filepath), exist_ok=True)
        yahoo_df.to_csv(output_filepath, index=False)
        print(f"Successfully created Yahoo Finance import file at: {output_filepath}")

    except FileNotFoundError:
        print(f"Error: The file '{input_filepath}' was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))

        data_dir = os.path.join(script_dir, "data", "activity_statement_parts")
        input_filepath = os.path.join(data_dir, "trades.csv")

        output_dir = os.path.join(script_dir, "data")
        output_filepath = os.path.join(output_dir, "yahoo_finance_import.csv")

        format_trades_for_yahoo(input_filepath, output_filepath)
    except NameError:
        print(
            "Could not determine script directory. Please run this script from a file."
        )
