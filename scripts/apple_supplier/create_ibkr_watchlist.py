#!/usr/bin/env python3
"""
Create IBKR watchlist for Apple suppliers using the generic IBKRManager class.
"""

from pathlib import Path

from app.ibkr import IBKRManager


def main():
    """Create Apple suppliers watchlist in IBKR TWS."""
    print("Apple Suppliers IBKR Watchlist Creator")
    print("=" * 50)

    # Find ticker files in the data directory
    data_dir = Path(__file__).parent / "data"

    # Try to find ticker files in order of preference
    ticker_files = [
        data_dir / "ticker_list.txt",
        data_dir / "found_tickers_only.json",
        data_dir / "apple_supplier_tickers.csv",
    ]

    ticker_file = None
    for file_path in ticker_files:
        if file_path.exists():
            ticker_file = file_path
            break

    if not ticker_file:
        print("❌ No ticker files found!")
        print("Expected files:")
        for file_path in ticker_files:
            print(f"  - {file_path}")
        print("\nRun the Apple supplier parser first:")
        print("  uv run python parse_apple_suppliers.py")
        return False

    # Create IBKR manager
    manager = IBKRManager()

    try:
        # Test connection
        if not manager.test_connection():
            print("\n💡 Troubleshooting tips:")
            print("1. Make sure TWS or IB Gateway is running")
            print("2. Enable API connections in TWS/Gateway:")
            print("   - Configuration > API > Settings")
            print("   - Check 'Enable ActiveX and Socket Clients'")
            print("   - Uncheck 'Read-Only API' if checked")
            print("3. Verify the port (7497 for TWS, 4002 for Gateway)")
            return False

        # Load symbols from file
        symbols = manager.load_symbols_from_file(ticker_file)

        if not symbols:
            print("❌ No symbols found in file")
            return False

        print(f"Found {len(symbols)} symbols to add to watchlist")

        # Create watchlist
        results = manager.create_watchlist_from_symbols(
            symbols=symbols, watchlist_name="Apple_Suppliers", output_dir=data_dir
        )

        if results["success"]:
            print("\n🎉 Apple Suppliers watchlist created successfully!")
            print(f"✅ Added {len(results['successful_contracts'])} contracts to TWS")

            if results["failed_symbols"]:
                print(f"⚠️  {len(results['failed_symbols'])} symbols could not be added")
                print("Check the failed symbols file for details")

            print("\n🎯 WATCHLIST CREATED! Next steps:")
            print("1. In TWS: File → Import → Import Watchlist")
            print("2. Select the TWS import file that was just created")
            print("3. Name your watchlist 'Apple Suppliers' when prompted")
            print("4. Your watchlist will appear in TWS with all symbols!")
            print("\nAlternatively:")
            print("- Check your TWS for individual market data lines")
            print("- Set up alerts, scanners, or other monitoring as needed")

        return results["success"]

    except Exception as e:
        print(f"❌ Error: {e}")
        return False

    finally:
        manager.disconnect()


if __name__ == "__main__":
    main()
