from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as ChromeOptions

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

    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument("--headless")
    chrome_options.add_argument('log-level=3')
    # chrome_options.add_experimental_option(
    #     'excludeSwitches', ['enable-logging'])

    return CustomChrome(options=chrome_options)


class CSV_Queue:
    def __init__(self, path):
        self.path = path
        try:
            print('Reading output CSV file...', end=" ")
            def date_converter(date): return dateparser.parse(date).date()
            self.data = pd.read_csv(
                self.path,
                converters={
                    "scrape_date": date_converter,
                    "rental_date": date_converter,
                    "availability_updated": date_converter
                }
            )
            if not set(['scrape_date', 'name', 'cleaning_fee', 'property_id', 'rental_date',
                        'availability_updated', 'rent_night', 'min_stay', 'availability']).issubset(self.data.columns):
                raise SyntaxError

            print('Done reading.')
        except FileNotFoundError:
            print('The file doesn\'t exist. Creating new one.')
            self.data = pd.DataFrame(columns=['scrape_date', 'name', 'cleaning_fee', 'property_id', 'rental_date',
                                              'availability_updated', 'rent_night', 'min_stay', 'availability'])

    def add(self, property_id, rental_date, availability_updated, name, cleaning_fee, rent_night, min_stay, availability):
        scrape_date = datetime.now().date()
        if not self.already_in_queue(property_id, rental_date, scrape_date, availability_updated):
            new_row = [scrape_date, name, cleaning_fee, property_id, rental_date,
                       availability_updated, rent_night, min_stay, availability]
            self.data.loc[len(self.data)] = new_row
            return True
        else:
            return False

    def already_in_queue(self, property_id, rental_date, scrape_date, availability_updated):
        return not self.data.loc[(self.data['property_id'] == property_id)
                                 & (self.data['rental_date'] == rental_date)
                                 & (self.data['scrape_date'] == scrape_date)
                                 & (self.data['availability_updated'] == availability_updated)].empty

    def to_csv_file(self):
        return self.data.to_csv(self.path, index=False)

    def __len__(self):
        return len(self.data.index)
