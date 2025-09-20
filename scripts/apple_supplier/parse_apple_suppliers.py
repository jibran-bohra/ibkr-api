#!/usr/bin/env python3
"""
Apple Supplier List Parser
Downloads and parses Apple's supplier list PDF to extract company names and find their stock tickers.
"""

import io
import json
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import pdfplumber
import requests
from tqdm import tqdm
from yahooquery import search


class AppleSupplierParser:
    def __init__(self):
        self.pdf_url = "https://s203.q4cdn.com/367071867/files/doc_downloads/2024/04/Apple-Supplier-List.pdf"
        self.suppliers = []
        self.tickers = {}
        self.progress_lock = threading.Lock()

    def download_and_parse_pdf(self) -> List[str]:
        """Download PDF into memory and extract supplier company names."""
        suppliers = []

        try:
            print("Downloading and parsing Apple supplier list PDF...")

            # Download PDF into memory
            response = requests.get(self.pdf_url)
            response.raise_for_status()

            # Create a BytesIO object from the PDF content
            pdf_buffer = io.BytesIO(response.content)

            with pdfplumber.open(pdf_buffer) as pdf:
                print(f"Processing {len(pdf.pages)} pages...")

                for page_num, page in enumerate(pdf.pages, 1):
                    print(f"  Extracting tables from page {page_num}")

                    # Extract tables from the page
                    tables = page.extract_tables()

                    if tables:
                        for table_num, table in enumerate(tables):
                            print(
                                f"    Found table {table_num + 1} with {len(table)} rows"
                            )

                            for row in table:
                                if row:  # Skip empty rows
                                    for cell in row:
                                        if cell and isinstance(cell, str):
                                            cell = cell.strip()

                                            # Skip empty cells and obvious non-company entries
                                            if (
                                                not cell
                                                or not self.is_likely_company_name(cell)
                                            ):
                                                continue

                                            skip_patterns = [
                                                r"^page \d+",
                                                r"^www\.",
                                                r"@.*\.",
                                                r"©",
                                                r"fiscal year",
                                                r"trademarks",
                                                r"apple inc\.",
                                                r"^[A-Z]{2,3}$",  # Country codes
                                                r"^\d{4}$",  # Just years
                                            ]

                                            # Check if it contains long descriptive text (likely headers/footers)
                                            if (
                                                len(cell.split()) > 10
                                            ):  # More than 10 words is likely descriptive text
                                                continue

                                            # Check patterns
                                            if any(
                                                re.search(pattern, cell, re.IGNORECASE)
                                                for pattern in skip_patterns
                                            ):
                                                continue

                                            # Check if this looks like a company name
                                            if self.is_likely_company_name(cell):
                                                clean_name = self.clean_company_name(
                                                    cell
                                                )
                                                suppliers.append(clean_name)

            # Remove duplicates while preserving order
            suppliers = list(dict.fromkeys(suppliers))

            print(f"Extracted {len(suppliers)} potential supplier names")
            return suppliers

        except Exception as e:
            print(f"Error downloading or parsing PDF: {e}")
            return []

    def is_likely_company_name(self, text: str) -> bool:
        """Check if a text string is likely to be a company name."""
        if not text or len(text.strip()) < 1:
            return False

        text = text.strip()

        # Special cases for known short company names
        known_short_companies = ["3M", "HP", "LG", "SK", "AT&T", "IBM", "AMD", "ARM"]
        if text in known_short_companies:
            return True

        # Skip obvious non-company patterns
        non_company_patterns = [
            r"^\d+$",  # Just numbers (but not alphanumeric like 3M)
            r"^Page \d+",  # Page numbers
            r"^www\.",  # URLs
            r"@.*\.",  # Email addresses
            r"^Manufacturing Site",  # Headers
            r"^fiscal",  # Headers
            r"^trademark",  # Headers
            r"^Company Name",  # Headers
            r"^Apple Inc",  # Apple references
            r"^List",  # Apple references
            r"©",  # Copyright
            r"^\d{1,2}/\d{1,2}/\d{4}$",  # Dates
            r"^[A-Z][a-z]+,\s[A-Z]{2}$",  # City, State format
            r"^[A-Z]{2}$",  # Two letter country codes (but not companies like HP)
        ]

        for pattern in non_company_patterns:
            if re.match(pattern, text, re.IGNORECASE):
                return False

        # Positive indicators for company names
        company_indicators = [
            r".*(?:Corp|Corporation|Inc|Incorporated|Ltd|Limited|Co\.|Company|LLC|Group|Holdings|Technologies|Systems|Solutions|Industries|Manufacturing|Electronics|Semiconductor).*",
            r".*(?:Precision|Micro|Tech|Electric|Digital|Global|International|Advanced|Automotive|Industrial).*",
        ]

        # Check corporate suffixes
        for pattern in company_indicators:
            if re.match(pattern, text, re.IGNORECASE):
                return True

        # Check if it looks like a proper company name
        # Allow shorter names (1-2 characters) if they contain letters and numbers (like 3M)
        if re.match(r"^[A-Z0-9][A-Za-z0-9\s&\-\.\,\(\)]*$", text):
            # For very short names, be more restrictive
            if len(text) <= 3:
                # Allow if it contains both letters and numbers, or is all caps with letters
                if (
                    re.search(r"[A-Za-z]", text) and re.search(r"[0-9]", text)
                ) or text.isupper():
                    return True
            # For longer names, allow if reasonable length
            elif 1 <= len(text.split()) <= 8 and len(text) <= 100:
                return True

        return False

    def clean_company_name(self, name: str) -> str:
        """Clean and normalize company names."""
        # Remove extra whitespace
        name = re.sub(r"\s+", " ", name.strip())

        # Remove page numbers and other artifacts
        name = re.sub(r"^\d+\s+", "", name)

        # Remove common PDF artifacts
        artifacts = ["●", "•", "▪", "▫", "◦"]
        for artifact in artifacts:
            name = name.replace(artifact, "")

        return name.strip()

    def search_ticker(self, company_name: str) -> Optional[str]:
        """Search for a company's stock ticker using yahooquery."""
        try:
            # Create search variations
            search_terms = []

            # Original name
            search_terms.append(company_name)

            # Remove common corporate suffixes
            clean_name = re.sub(
                r"\b(Corp|Corporation|Inc|Incorporated|Ltd|Limited|Co\.|Company|LLC|Group|Holdings|Technologies|Tech|Systems|Solutions|Industries|Manufacturing|Electronics|Semiconductor)\b",
                "",
                company_name,
                flags=re.IGNORECASE,
            ).strip()
            if clean_name and clean_name != company_name:
                search_terms.append(clean_name)

            # First word only (often the main brand)
            first_word = company_name.split()[0]
            if len(first_word) > 2:
                search_terms.append(first_word)

            for term in search_terms:
                if len(term) < 2:
                    continue

                try:
                    # Use yahooquery search
                    results = search(term)

                    if results and "quotes" in results and len(results["quotes"]) > 0:
                        # Get the first match
                        quote = results["quotes"][0]
                        if "symbol" in quote and quote["symbol"]:
                            # Verify it's a stock (not ETF, option, etc.)
                            if quote.get("quoteType", "").upper() == "EQUITY":
                                return quote["symbol"]
                            # If no quoteType, still return the symbol
                            elif "quoteType" not in quote:
                                return quote["symbol"]

                except Exception as e:
                    print(f"Error searching ticker for {term}: {e}")
                    continue

            return None

        except Exception as e:
            print(f"Error searching ticker for {company_name}: {e}")
            return None

    def search_ticker_with_progress(self, supplier_info: tuple) -> tuple:
        """Search for ticker with progress tracking - thread-safe wrapper."""
        index, supplier = supplier_info

        # Add small delay to avoid overwhelming the API
        time.sleep(0.1)

        ticker = self.search_ticker(supplier)

        return (supplier, ticker)

    def find_tickers(self, max_workers: int = 10) -> Dict[str, str]:
        """Find stock tickers for all suppliers using multithreading."""
        print(f"Searching for stock tickers using {max_workers} threads...")
        tickers = {}

        # Create list of (index, supplier) tuples for progress tracking
        supplier_items = list(enumerate(self.suppliers))

        # Use ThreadPoolExecutor for concurrent ticker searches with progress bar
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_supplier = {
                executor.submit(self.search_ticker_with_progress, item): item[1]
                for item in supplier_items
            }

            # Process completed tasks with progress bar
            completed_count = 0
            found_count = 0

            with tqdm(
                total=len(supplier_items),
                desc="Finding tickers",
                bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
            ) as pbar:
                for future in as_completed(future_to_supplier):
                    try:
                        supplier, ticker = future.result()
                        completed_count += 1

                        if ticker:
                            tickers[supplier] = ticker
                            found_count += 1
                        else:
                            # Store None for suppliers without tickers
                            tickers[supplier] = None

                        # Update description with stable counts instead of variable text
                        pbar.set_description(
                            f"Finding tickers (found: {found_count}/{completed_count})"
                        )

                    except Exception as e:
                        supplier = future_to_supplier[future]
                        completed_count += 1
                        tickers[supplier] = None  # Store None for errors too
                        pbar.set_description(
                            f"Finding tickers (found: {found_count}/{completed_count})"
                        )
                        print(f"Error processing {supplier}: {e}")
                    finally:
                        pbar.update(1)

        return tickers

    def save_results(self):
        """Save the results to files in the data/ directory."""
        # Create data directory relative to the script location
        script_dir = Path(__file__).parent
        data_dir = script_dir / "data"
        data_dir.mkdir(exist_ok=True)

        # Save all suppliers to text file
        with open(data_dir / "apple_suppliers.txt", "w") as f:
            for supplier in self.suppliers:
                f.write(f"{supplier}\n")

        # Save tickers to JSON
        with open(data_dir / "apple_supplier_tickers.json", "w") as f:
            json.dump(self.tickers, f, indent=2)

        # Save tickers to CSV
        df = pd.DataFrame(
            [
                {"Company": company, "Ticker": ticker}
                for company, ticker in self.tickers.items()
            ]
        )
        df.to_csv(data_dir / "apple_supplier_tickers.csv", index=False)

        # Save just the ticker list (only non-None tickers)
        with open(data_dir / "ticker_list.txt", "w") as f:
            for ticker in self.tickers.values():
                if ticker is not None:
                    f.write(f"{ticker}\n")

        # Save only found tickers to a separate file
        found_tickers = {k: v for k, v in self.tickers.items() if v is not None}
        with open(data_dir / "found_tickers_only.json", "w") as f:
            json.dump(found_tickers, f, indent=2)

        found_count = len([t for t in self.tickers.values() if t is not None])

        print(f"\nResults saved to {data_dir}:")
        print(f"  All suppliers: apple_suppliers.txt ({len(self.suppliers)} companies)")
        print(
            f"  All results (JSON): apple_supplier_tickers.json ({len(self.tickers)} entries)"
        )
        print("  All results (CSV): apple_supplier_tickers.csv")
        print(f"  Found tickers only: found_tickers_only.json ({found_count} tickers)")
        print(f"  Ticker list: ticker_list.txt ({found_count} tickers)")

    def run(self):
        """Main execution method."""
        print("Apple Supplier List Parser")
        print("=" * 50)

        # Download and parse PDF in memory
        self.suppliers = self.download_and_parse_pdf()
        if not self.suppliers:
            print("No suppliers found in PDF")
            return False

        # Find tickers
        self.tickers = self.find_tickers()

        # Save results
        self.save_results()

        found_tickers = len([t for t in self.tickers.values() if t is not None])

        print("\nSummary:")
        print(f"  Total suppliers extracted: {len(self.suppliers)}")
        print(f"  Tickers found: {found_tickers}")
        print(f"  Success rate: {found_tickers / len(self.suppliers) * 100:.1f}%")

        return True


if __name__ == "__main__":
    parser = AppleSupplierParser()
    parser.run()
