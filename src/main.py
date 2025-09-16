#!/usr/bin/env python3
"""
AirLife ETL Pipeline - Simple Version
"""

from src.extract_data import extract_airports, extract_flights
from src.transform_data import clean_airports, clean_flights, combine_data
from src.load_data import load_to_database, verify_data

def main():
    """Run the complete ETL pipeline"""
    print("Starting AirLife ETL Pipeline...")

    # Step 1: Extract data
    print("\n=== EXTRACTION ===")
    airports = extract_airports()
    flights = extract_flights()

    # Step 2: Transform data
    print("\n=== TRANSFORMATION ===")
    clean_airports_data = clean_airports(airports)
    clean_flights_data = clean_flights(flights)
    final_airports, final_flights = combine_data(clean_airports_data, clean_flights_data)

    # Step 3: Load data
    print("\n=== LOADING ===")
    load_to_database(final_airports, final_flights)

    # Step 4: Verify everything worked
    print("\n=== VERIFICATION ===")
    verify_data()

    print("\n✅ ETL Pipeline completed!")

if __name__ == "__main__":
    main()