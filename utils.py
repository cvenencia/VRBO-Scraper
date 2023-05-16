from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

import pandas as pd
from datetime import datetime
import dateparser


class CustomChrome(Chrome):
    def select(self, selector):
        return self.find_element(By.CSS_SELECTOR, selector)

    def select_all(self, selector):
        return self.find_elements(By.CSS_SELECTOR, selector)

    def get_variable(self, variable):
        return self.execute_script(f"return {variable}")


def get_driver():
    chrome_options = ChromeOptions()
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"

    caps = DesiredCapabilities().CHROME
    caps['pageLoadStrategy'] = 'eager'
    chrome_options.headless = True
    chrome_options.add_argument(f'user-agent={user_agent}')
    chrome_options.add_argument('log-level=3')
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--allow-running-insecure-content')
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--proxy-server='direct://'")
    chrome_options.add_argument("--proxy-bypass-list=*")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--lang=en-GB')
    # chrome_options.add_experimental_option(
    #     'excludeSwitches', ['enable-logging'])

    return CustomChrome(options=chrome_options, desired_capabilities=caps)
    return CustomChrome(options=chrome_options)


class CSV_Queue:
    def __init__(self, output_path, dates_path):
        self.output_path = output_path
        self.dates_path = dates_path
        self.columns = ['scrape_date', 'cleaning_fee', 'property_id', 'source', 'rental_date',
                        'availability_updated', 'rent_night', 'average_rent_night', 'min_stay', 'availability', 'status', 'day_of_week', 'weblink', 'name']

        def date_converter(date):
            if date != '':
                return dateparser.parse(date).date()
        try:
            print('Reading output CSV file...')
            self.data = pd.read_csv(
                self.output_path,
                converters={
                    "scrape_date": date_converter,
                    "rental_date": date_converter,
                    "availability_updated": date_converter
                }
            )
            if not set(self.columns).issubset(self.data.columns):
                raise SyntaxError
            print('Done reading.')

        except FileNotFoundError:
            print('The file doesn\'t exist. Creating new one.')
            self.data = pd.DataFrame(columns=self.columns)

        print('Reading dates CSV file...')
        self.dates = pd.read_csv(self.dates_path, converters={
                                 'dates': date_converter})
        print('Done reading')

    def get_status(self, availability, property_id, rental_date, scrape_date):
        if availability:
            return 'available'
        filtered_data = self.data.loc[(self.data['property_id'] == property_id) & (
            self.data['rental_date'] == rental_date) & (self.data['scrape_date'] != scrape_date)]
        if (filtered_data['availability'] == True).any():
            return 'likely rented'
        else:
            return 'not listed'

    def get_day_of_week(self, date):
        if not (self.dates.loc[self.dates['dates'] == date]).empty:
            return 'holiday'
        else:
            weekday = date.weekday()
            if weekday == 0:
                return 'monday'
            elif weekday == 1:
                return 'tuesday'
            elif weekday == 2:
                return 'wednesday'
            elif weekday == 3:
                return 'thursday'
            elif weekday == 4:
                return 'friday'
            elif weekday == 5:
                return 'saturday'
            elif weekday == 6:
                return 'sunday'

    def get_weblink(self, source, property_id):
        if source == 'vacasa':
            return f'https://vacasa.com/unit/{property_id}'
        else:
            return f'https://vrbo.com/{property_id}'

    def add(self, source, property_id, rental_date, availability_updated, name, cleaning_fee, average_rent_night, rent_night, min_stay, availability):
        scrape_date = datetime.now().date()
        if not self.already_in_queue(source, property_id, rental_date, scrape_date, availability_updated):
            status = self.get_status(
                availability, property_id, rental_date, scrape_date)
            day_of_week = self.get_day_of_week(rental_date)
            weblink = self.get_weblink(source, property_id)
            new_row = [scrape_date, cleaning_fee, property_id, source, rental_date,
                       availability_updated, rent_night, average_rent_night, min_stay, availability, status, day_of_week, weblink, name]
            self.data.loc[len(self.data)] = new_row
            return True
        else:
            return False

    def already_in_queue(self, source, property_id, rental_date, scrape_date, availability_updated):
        if source == 'vrbo':
            return not self.data.loc[(self.data['source'] == source)
                                     & (self.data['property_id'] == property_id)
                                     & (self.data['rental_date'] == rental_date)
                                     & (self.data['scrape_date'] == scrape_date)
                                     & (self.data['availability_updated'] == availability_updated)].empty
        elif source == 'vacasa':
            return not self.data.loc[(self.data['source'] == source)
                                     & (self.data['property_id'] == property_id)
                                     & (self.data['rental_date'] == rental_date)
                                     & (self.data['scrape_date'] == scrape_date)].empty

    def to_csv_file(self):
        return self.data.to_csv(self.output_path, index=False)

    def __len__(self):
        return len(self.data.index)
