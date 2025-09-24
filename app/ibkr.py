import json
import time
from pathlib import Path
from typing import Dict, List, Optional, Union

import pandas as pd
from ib_insync import Contract, IB, Stock
from tqdm import tqdm


class IBKRManager:
    def __init__(self, host: str = "127.0.0.1", port: int = 7495, client_id: int = 1):
        """
        Initialize IBKR connection.

        Args:
            host: TWS/Gateway host (default: 127.0.0.1)
            port: TWS port (7495 for TWS, 4002 for Gateway)
            client_id: Unique client ID for this connection
        """
        self.ib = IB()
        self.host = host
        self.port = port
        self.client_id = client_id
        self.connected = False

    def connect(self) -> bool:
        """Connect to TWS/Gateway."""
        try:
            print(f"Connecting to IBKR TWS at {self.host}:{self.port}...")
            self.ib.connect(self.host, self.port, clientId=self.client_id, timeout=20)
            self.connected = True
            print("✅ Connected to IBKR TWS")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to IBKR TWS: {e}")
            print("\nMake sure:")
            print("1. TWS or IB Gateway is running")
            print("2. API connections are enabled in TWS/Gateway")
            print("3. The correct port is specified (7497 for TWS, 4002 for Gateway)")
            return False

    def disconnect(self):
        """Disconnect from TWS/Gateway."""
        if self.connected:
            self.ib.disconnect()
            self.connected = False
            print("Disconnected from IBKR TWS")

    def create_contract(
        self,
        symbol: str,
        sec_type: str = "STK",
        exchange: str = "SMART",
        currency: str = "USD",
    ) -> Contract:
        """
        Create a contract for any security type.

        Args:
            symbol: Symbol/ticker
            sec_type: Security type (STK for stocks, OPT for options, etc.)
            exchange: Exchange (SMART for automatic routing)
            currency: Currency

        Returns:
            Contract object
        """
        if sec_type == "STK":
            return Stock(symbol, exchange, currency)
        else:
            # For other security types, create generic contract
            contract = Contract()
            contract.symbol = symbol
            contract.secType = sec_type
            contract.exchange = exchange
            contract.currency = currency
            return contract

    def qualify_contracts(
        self,
        symbols: List[str],
        sec_type: str = "STK",
        exchange: str = "SMART",
        currency: str = "USD",
        batch_size: int = 50,
    ) -> Dict[str, Optional[Contract]]:
        """
        Qualify contracts to ensure they exist and get proper contract details.

        Args:
            symbols: List of symbols/tickers
            sec_type: Security type (STK for stocks)
            exchange: Exchange
            currency: Currency
            batch_size: Number of contracts to process at once

        Returns:
            Dictionary mapping symbols to qualified contracts (None if not found)
        """
        if not self.connected:
            raise Exception("Not connected to IBKR TWS")

        print(f"Qualifying {len(symbols)} contracts...")

        contracts = []
        symbol_map = {}

        # Create contracts for all symbols
        for i, symbol in enumerate(symbols):
            contract = self.create_contract(symbol, sec_type, exchange, currency)
            contracts.append(contract)
            symbol_map[i] = symbol

        # Qualify contracts in batches
        qualified_contracts = {}

        with tqdm(total=len(contracts), desc="Qualifying contracts") as pbar:
            for i in range(0, len(contracts), batch_size):
                batch = contracts[i : i + batch_size]

                try:
                    # Qualify the batch
                    qualified_batch = self.ib.qualifyContracts(*batch)

                    # Process results
                    for j, original_contract in enumerate(batch):
                        contract_index = i + j  # Calculate the original index
                        symbol = symbol_map[contract_index]

                        # Find the qualified contract for this symbol
                        qualified = None
                        for q_contract in qualified_batch:
                            if (
                                q_contract.symbol == original_contract.symbol
                                and q_contract.currency == original_contract.currency
                                and q_contract.secType == original_contract.secType
                            ):
                                qualified = q_contract
                                break

                        qualified_contracts[symbol] = qualified

                        if qualified:
                            pbar.set_description(f"✅ {symbol}")
                        else:
                            pbar.set_description(f"❌ {symbol}")

                        pbar.update(1)

                    # Rate limiting
                    time.sleep(0.1)

                except Exception as e:
                    print(f"Error qualifying batch: {e}")
                    # Mark all in this batch as failed
                    for j, contract in enumerate(batch):
                        contract_index = i + j  # Calculate the original index
                        symbol = symbol_map[contract_index]
                        qualified_contracts[symbol] = None
                        pbar.update(1)

        successful = len([c for c in qualified_contracts.values() if c is not None])
        failed = len(qualified_contracts) - successful

        print("\nQualification complete:")
        print(f"  ✅ Successful: {successful}")
        print(f"  ❌ Failed: {failed}")

        return qualified_contracts

    def create_tws_importable_watchlist(
        self, contracts: List[Contract], watchlist_name: str, output_dir: Path
    ) -> str:
        """
        Create a CSV file that can be imported directly into TWS as a watchlist.

        Args:
            contracts: List of qualified contracts
            watchlist_name: Name for the watchlist
            output_dir: Directory to save the CSV file

        Returns:
            Path to the created CSV file
        """
        # Create TWS-compatible CSV format
        csv_data = []

        for contract in contracts:
            # TWS import format: Symbol,Exchange,Currency,SecType
            csv_data.append(
                {
                    "Symbol": contract.symbol,
                    "Exchange": contract.primaryExchange or contract.exchange,
                    "Currency": contract.currency,
                    "SecType": contract.secType,
                }
            )

        # Save to CSV
        df = pd.DataFrame(csv_data)
        csv_file = (
            output_dir / f"{watchlist_name.lower().replace(' ', '_')}_tws_import.csv"
        )
        df.to_csv(csv_file, index=False)

        return str(csv_file)

    def add_contracts_to_tws(
        self, contracts: List[Contract], request_market_data: bool = True
    ) -> int:
        """
        Add contracts to TWS for monitoring.

        Args:
            contracts: List of qualified contracts
            request_market_data: Whether to request market data (adds to TWS watchlist)

        Returns:
            Number of contracts successfully added
        """
        if not self.connected:
            raise Exception("Not connected to IBKR TWS")

        successful_count = 0

        print(f"Adding {len(contracts)} contracts to TWS...")

        with tqdm(total=len(contracts), desc="Adding contracts") as pbar:
            for contract in contracts:
                try:
                    if request_market_data:
                        # Request market data (this adds the contract to TWS)
                        self.ib.reqMktData(contract)

                    successful_count += 1
                    pbar.set_description(f"✅ {contract.symbol}")

                    # Small delay to avoid rate limits
                    time.sleep(0.05)

                except Exception as e:
                    pbar.set_description(f"❌ {contract.symbol}")
                    print(f"Warning: Could not add {contract.symbol}: {e}")

                pbar.update(1)

        print(
            f"✅ Successfully added {successful_count}/{len(contracts)} contracts to TWS"
        )
        return successful_count

    def create_watchlist_from_symbols(
        self,
        symbols: List[str],
        watchlist_name: str,
        sec_type: str = "STK",
        exchange: str = "SMART",
        currency: str = "USD",
        save_results: bool = True,
        output_dir: Optional[Path] = None,
    ) -> Dict[str, any]:
        """
        Create a watchlist from a list of symbols.

        Args:
            symbols: List of symbols/tickers
            watchlist_name: Name for the watchlist
            sec_type: Security type (STK for stocks)
            exchange: Exchange
            currency: Currency
            save_results: Whether to save results to files
            output_dir: Directory to save results (defaults to ./data)

        Returns:
            Dictionary with results including successful and failed symbols
        """
        if not self.connected:
            raise Exception("Not connected to IBKR TWS")

        print(f"Creating watchlist '{watchlist_name}' with {len(symbols)} symbols...")

        # Qualify contracts
        qualified_contracts = self.qualify_contracts(
            symbols, sec_type, exchange, currency
        )

        # Get only successfully qualified contracts
        valid_contracts = [
            contract
            for contract in qualified_contracts.values()
            if contract is not None
        ]

        if not valid_contracts:
            print("❌ No valid contracts found. Cannot create watchlist.")
            return {
                "success": False,
                "total_symbols": len(symbols),
                "successful_contracts": [],
                "failed_symbols": symbols,
            }

        # Add contracts to TWS
        added_count = self.add_contracts_to_tws(valid_contracts)

        # Create TWS-importable watchlist file
        csv_file = self.create_tws_importable_watchlist(
            valid_contracts, watchlist_name, output_dir or Path("./data")
        )

        # Prepare results
        results = {
            "success": added_count > 0,
            "watchlist_name": watchlist_name,
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "total_symbols": len(symbols),
            "successful_contracts": [],
            "failed_symbols": [],
            "tws_import_file": csv_file,
        }

        for symbol, contract in qualified_contracts.items():
            if contract:
                results["successful_contracts"].append(
                    {
                        "symbol": symbol,
                        "contract_symbol": contract.symbol,
                        "exchange": contract.primaryExchange or contract.exchange,
                        "currency": contract.currency,
                        "contract_id": contract.conId,
                        "sec_type": contract.secType,
                    }
                )
            else:
                results["failed_symbols"].append(symbol)

        # Save results if requested
        if save_results:
            self.save_watchlist_results(results, output_dir)

        print(
            f"✅ Watchlist '{watchlist_name}' created with {len(results['successful_contracts'])} contracts"
        )

        return results

    def save_watchlist_results(
        self, results: Dict[str, any], output_dir: Optional[Path] = None
    ):
        """
        Save watchlist creation results to files.

        Args:
            results: Results dictionary from create_watchlist_from_symbols
            output_dir: Directory to save files (defaults to ./data)
        """
        if output_dir is None:
            output_dir = Path("./data")

        output_dir.mkdir(exist_ok=True)

        watchlist_name = results["watchlist_name"]
        safe_name = watchlist_name.lower().replace(" ", "_").replace("-", "_")

        # Save complete results to JSON
        results_file = output_dir / f"watchlist_{safe_name}_results.json"
        with open(results_file, "w") as f:
            json.dump(results, f, indent=2)

        # Save successful contracts to CSV
        if results["successful_contracts"]:
            df = pd.DataFrame(results["successful_contracts"])
            csv_file = output_dir / f"watchlist_{safe_name}_contracts.csv"
            df.to_csv(csv_file, index=False)

        # Save failed symbols to text file
        if results["failed_symbols"]:
            failed_file = output_dir / f"watchlist_{safe_name}_failed.txt"
            with open(failed_file, "w") as f:
                for symbol in results["failed_symbols"]:
                    f.write(f"{symbol}\n")

        print(f"\nResults saved to {output_dir}:")
        print(f"  📄 Complete results: {results_file}")
        if results["successful_contracts"]:
            print(f"  📊 Contracts CSV: {csv_file}")
        if results["failed_symbols"]:
            print(f"  ❌ Failed symbols: {failed_file}")
        if results.get("tws_import_file"):
            print(f"  🎯 TWS Import File: {results['tws_import_file']}")
            print("     ↳ Import this file directly into TWS to create the watchlist!")

    def load_symbols_from_file(self, file_path: Union[str, Path]) -> List[str]:
        """
        Load symbols from various file formats.

        Args:
            file_path: Path to file containing symbols

        Returns:
            List of unique symbols
        """
        file_path = Path(file_path)
        symbols = []

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        if file_path.suffix.lower() == ".json":
            with open(file_path) as f:
                data = json.load(f)
                if isinstance(data, dict):
                    # Assume it's a mapping, get non-null values
                    symbols = [
                        str(ticker) for ticker in data.values() if ticker is not None
                    ]
                elif isinstance(data, list):
                    symbols = [str(item) for item in data if item is not None]

        elif file_path.suffix.lower() == ".csv":
            df = pd.read_csv(file_path)
            # Look for common column names
            for col_name in ["Ticker", "ticker", "Symbol", "symbol"]:
                if col_name in df.columns:
                    symbols = df[col_name].dropna().astype(str).tolist()

                    break

        elif file_path.suffix.lower() == ".txt":
            with open(file_path) as f:
                symbols = [line.strip() for line in f if line.strip()]

        else:
            raise ValueError(f"Unsupported file format: {file_path.suffix}")

        # Remove duplicates and empty strings, then sort
        symbols = sorted(list(set([s for s in symbols if s and s.strip()])))
        symbols = [s.split(".")[0] for s in symbols]
        print(f"Loaded {len(symbols)} unique symbols from {file_path}")
        return symbols

    def test_connection(self) -> bool:
        """
        Test the connection and basic functionality.

        Returns:
            True if connection test passes, False otherwise
        """
        if not self.connected:
            if not self.connect():
                return False

        try:
            print("Testing contract qualification with AAPL...")
            test_contracts = self.qualify_contracts(["AAPL"])

            if test_contracts.get("AAPL"):
                print("✅ Connection test successful!")
                return True
            else:
                print("❌ Connection test failed - could not qualify AAPL")
                return False

        except Exception as e:
            print(f"❌ Connection test failed: {e}")
            return False
