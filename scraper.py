import argparse
import pandas as pd
from utils import get_driver, CSV_Queue

import dateparser
from datetime import timedelta

parser = argparse.ArgumentParser()
parser.add_argument("--begin-date", help="Begin date")
parser.add_argument("--end-date", help="End date")
parser.add_argument("--csv-input-file", help="Path to csv input file")
parser.add_argument("--csv-output-file", help="Path to csv output file")

args = parser.parse_args()
missing_args = False
if not args.begin_date:
    print('Missing required argument: --begin-date. Date format: "YYYY-MM-DD".')
    missing_args = True

if not args.end_date:
    print('Missing required argument: --end-date. Date format: "YYYY-MM-DD".')
    missing_args = True

if not args.csv_input_file:
    print('Missing required argument: --csv-input-file.')
    missing_args = True

if missing_args:
    exit(1)

try:
    begin_date = dateparser.parse(args.begin_date).date()
except AttributeError:
    print('ERROR: Invalid begin date.')
    exit(1)
try:
    end_date = dateparser.parse(args.end_date).date()
except AttributeError:
    print('ERROR: Invalid end date.')
    exit(1)

if begin_date > end_date:
    print('ERROR: Invalid range of dates.')
    exit(1)

csv_input_path = args.csv_input_file
try:
    codes = pd.read_csv(csv_input_path)['codes']
except FileNotFoundError:
    print('ERROR: CSV file does not exist.')
    exit(1)
except pd.errors.ParserError:
    print('ERROR: Invalid input CSV file.')
    exit(1)
except KeyError:
    print('ERROR: CSV file does not contain a column "codes".')
    exit(1)

csv_output_path = args.csv_output_file or "data.csv"

try:
    csv_queue = CSV_Queue(csv_output_path)
except pd.errors.ParserError:
    print('\nERROR: Invalid output CSV file.')
    exit(1)
except SyntaxError:
    print('\nERROR: this CSV file has the wrong structure. Please provide a valid file, if not, provide a path to a non-existing file.')
    exit(1)

try:
    driver = get_driver()
    print()
    for code in codes:

        def scrape_page():
            try:
                availability = driver.get_variable(
                    'window.__INITIAL_STATE__.listingReducer.availabilityCalendar.availability.unitAvailabilityConfiguration.availability')
                min_stay = driver.get_variable(
                    'window.__INITIAL_STATE__.listingReducer.availabilityCalendar.availability.unitAvailabilityConfiguration.minStay').split(',')
                begin_date_availability = dateparser.parse(driver.get_variable(
                    'window.__INITIAL_STATE__.listingReducer.availabilityCalendar.availability.dateRange.beginDate')).date()
                end_date_availability = dateparser.parse(driver.get_variable(
                    'window.__INITIAL_STATE__.listingReducer.availabilityCalendar.availability.dateRange.endDate')).date()
                availability_updated = dateparser.parse(driver.get_variable(
                    'window.__INITIAL_STATE__.listingReducer.availabilityUpdated')).date()
                name = driver.get_variable(
                    'window.__INITIAL_STATE__.listingReducer.headline')

                rent_nights = driver.get_variable(
                    'window.__INITIAL_STATE__.listingReducer.rateSummary.rentNights')
                average_rent_night = float(driver.select(
                    '.rental-price__amount').get_attribute('textContent').replace('$', ''))
                begin_date_rent_nights = dateparser.parse(driver.get_variable(
                    'window.__INITIAL_STATE__.listingReducer.rateSummary.beginDate')).date()
                end_date_rent_nights = dateparser.parse(driver.get_variable(
                    'window.__INITIAL_STATE__.listingReducer.rateSummary.endDate')).date()
                flat_fees = driver.get_variable(
                    'window.__INITIAL_STATE__.listingReducer.rateSummary.flatFees')
            except Exception as e:
                print(f'  There was an error scraping this page: {e}\n')
                return False

            cleaning_fee_min = None
            cleaning_fee_max = None
            for fee in flat_fees:
                if fee['type'] != 'CLEANING_FEE':
                    continue
                cleaning_fee_min = fee['minAmount']
                cleaning_fee_max = fee['maxAmount']

            info_dates = {}
            d = begin_date_availability
            for (is_available, min_stayy) in zip(availability, min_stay):
                info_dates[d] = {
                    "availability": True if is_available == 'Y' else False,
                    "min_stay": int(min_stayy),
                    "rent_night": None
                }
                d += timedelta(days=1)

            if rent_nights == None:
                print('  WARNING: rentNights is null.')
            else:
                d = begin_date_rent_nights
                for rent_night in rent_nights:
                    if info_dates.get(d):
                        info_dates[d]['rent_night'] = rent_night
                    else:
                        info_dates[d] = {'rent_night': rent_night,
                                         "availability": None, "min_stay": None}
                    d += timedelta(days=1)

            return info_dates, availability_updated, name, cleaning_fee_min, average_rent_night

        def add_to_csv_file(info_dates, availability_updated, name, cleaning_fee, average_rent_night):
            added_count = 0
            ignored_count = 0
            date = begin_date
            while date <= end_date:
                if info_dates.get(date):
                    was_added = csv_queue.add(
                        code,
                        date,
                        availability_updated,
                        name,
                        cleaning_fee,
                        average_rent_night,
                        **info_dates[date]
                    )
                    if was_added:
                        added_count += 1
                    else:
                        ignored_count += 1
                date += timedelta(days=1)

            return added_count, ignored_count

        url = f'https://www.vrbo.com/{code}'
        driver.get(url)
        print(f'Scraping {url}...')
        data = scrape_page()
        if not data:
            continue
        added_count, ignored_count = add_to_csv_file(*data)
        print(f'  Name: {data[2]}')
        print(
            f'  {added_count} record{"s were" if added_count != 1 else " was"} added from this page.')
        print(
            f'  {ignored_count} record{"s were" if ignored_count != 1 else " was"} ignored because they were already added.')
        print()
        csv_queue.to_csv_file()

    print('Scraping process done!')

    driver.quit()
    csv_queue.to_csv_file()
except KeyboardInterrupt:
    print('Keyboard Interrupt detected.')
except Exception as e:
    print(f'An unexpected error ocurred: {e}')
finally:
    print('Closing Chrome driver...')
    driver.quit()
