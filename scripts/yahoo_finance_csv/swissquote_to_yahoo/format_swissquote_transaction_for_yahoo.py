import pandas as pd
import os


def format_swissquote_transactions_for_yahoo(input_filepath, output_filepath):
    """
    Formats a Swissquote transactions CSV file for Yahoo Finance import.
    """
    try:
        # Read the CSV file, specifying the delimiter
        df = pd.read_csv(input_filepath, delimiter=";")

        # Consider 'Buy', 'Sell', and 'Crypto Deposit' as transactions
        df = df[df["Transaction"].isin(["Buy", "Sell", "Crypto Deposit"])]

        # Rename columns for Yahoo Finance compatibility
        rename_map = {
            "Symbol": "Symbol",
            "Date": "Trade Date",
            "Quantity": "Quantity",
            "Unit price": "Purchase Price",
            "Costs": "Commission",
            "Transaction": "Transaction Type",
        }

        # Create a new DataFrame with only the required columns and rename them
        yahoo_df = df[list(rename_map.keys())].copy()
        yahoo_df.rename(columns=rename_map, inplace=True)

        # Convert 'Trade Date' to the required format (YYYY-MM-DD)
        yahoo_df["Trade Date"] = pd.to_datetime(
            yahoo_df["Trade Date"], format="%d-%m-%Y %H:%M:%S"
        ).dt.strftime("%Y%m%d")

        # # Adjust 'Purchase Price' to be negative for 'Sell'
        # yahoo_df["Purchase Price"] = yahoo_df.apply(
        #     lambda row: -row["Purchase Price"]
        #     if row["Transaction Type"].lower() == "sell"
        #     else row["Purchase Price"],
        #     axis=1,
        # )
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

        # Replace 'BTC' with 'BTC-USD' and 'ETH' with 'ETH-USD' in the 'Symbol' column
        yahoo_df["Symbol"] = yahoo_df["Symbol"].replace(
            {"BTC": "BTC-USD", "ETH": "ETH-USD"}
        )

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

        # Save the formatted data to a new CSV file
        os.makedirs(os.path.dirname(output_filepath), exist_ok=True)
        yahoo_df.to_csv(output_filepath, index=False)
        print(f"Successfully created Yahoo Finance import file at: {output_filepath}")

    except FileNotFoundError:
        print(f"Error: The file '{input_filepath}' was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    try:
        # Determine the directory of the script
        script_dir = os.path.dirname(os.path.abspath(__file__))

        # Define the input and output file paths
        data_dir = os.path.join(script_dir, "data")
        input_filepath = os.path.join(
            data_dir, "transactions-from-14091999-to-01092025.csv"
        )
        output_filepath = os.path.join(data_dir, "yahoo_finance_import.csv")

        # Run the formatting function
        format_swissquote_transactions_for_yahoo(input_filepath, output_filepath)
    except NameError:
        print(
            "Could not determine script directory. Please run this script from a file."
        )
