import numpy as np
import pandas as pd

import re
import yaml

from dateutil.parser import parse
from datetime import datetime


class DataCleaning:
    '''
    A class that handles all the data cleaning for different dataframes.

    Methods:
    -------
    clean_user_data(user_data_df: pd.DataFrame)
        Cleans the user_data and returns the clean data as a dataframe.
    clean_card_data(card_data_df: pd.DataFrame)
        Cleans the card_data and returns the clean data as a dataframe.
    clean_store_data(store_data_df: pd.DataFrame)
        Cleans the store_data and returns the clean data as a dataframe.
    convert_product_weights(product_data_df: pd.DataFrame)
        Converts the weights of all the products in a dataframe into kg and returns the converted dataframe.
    clean_products_data(product_data_df: pd.DataFrame)
        Cleans the product_data and returns the clean data as a dataframe.
    clean_orders_data(order_data_df: pd.DataFrame)
        Cleans the order_data and returns the clean data as a dataframe.
    clean_events_data(event_data_df: pd.DataFrame)
        Cleans the event_data and returns the clean data as a dataframe.

    Static Methods:
    safe_parse(date_string: str)
        Safely parses a date avoiding ValueError. If the string contains a valid date, it will return it as a parsed date. Otherwise, if
        will return np.nan (instead of throwing a ValueError).
    '''
    
    def clean_user_data(self, user_data_df: pd.DataFrame):
        '''
        Cleans the user data.

        Parameters
        ----------
        user_data_df : pd.DataFrame
            The user data to be cleaned.
            The data must contain the columns: first_name, last_name, date_of_birth, company, email_address, address, country, country_code, phone_number, join_date, user_uuid.
            
        Returns
        -------
        df: pd.Dataframe
            The cleaned user data.
        '''
        
        df = user_data_df.copy()

        if 'index' in df.columns:
            df = df.set_index('index')

        # apply .astype('string') to several columns
        df.first_name, df.last_name, df.company, df.email_address, df.address, df.country, df.country_code = map(lambda x: x.astype('string'), (df.first_name, df.last_name, df.company, df.email_address, df.address, df.country, df.country_code))

        # clean dates: some dates were null, some were a string of alphanumerical characters, and many were valid but in mixed formats
        # The valid dates returned a proper date when passed to `parse`, the rest threw a ValueError. So we define safe_parse to deal with this.
        df.date_of_birth = df.date_of_birth.apply(self.safe_parse)
        df.join_date = df.join_date.apply(self.safe_parse)
        df.dropna(inplace = True) # also removes all lines with 'NULL' due to correlation

        # clean country codes : some entries were 'GGB', upon inspection they looked like they're were all from the UK,so replace it with 'GB'.
        df['country_code'][df['country_code'] == 'GGB'] = 'GB'

        # clean email addresses: some of the email addresses had double @s, replace it with a single @.
        df['email_address'] = df['email_address'].apply(lambda x: x.replace('@@','@'))

        # clean phone numbers: the formats of each number are going to vary from country to country, so we focus on cleaning the numbers themselves
        # we found a lot of numbers with dots and x's. The numbers look like proper numbers otherwise, so we decide to just drop the dots and x's instead of the whole row
        df['phone_number'] = df['phone_number'].apply(lambda x: re.sub(r'x|\.', '', x))

        return df
    
    def clean_card_data(self, card_data_df: pd.DataFrame):
        '''
        Cleans the card data.

        Parameters
        ----------
        card_data_df : pd.DataFrame
            The card data to be cleaned.
            The dataframe must contain the columns: card_number, expiry_date, card_provider, date_payment_confirmed.
            
        Returns
        -------
        df: pd.Dataframe
            The cleaned card data.
        '''

        df = card_data_df.copy()

        # Clean expiry_date
        # A limited amount of entries are not of the format dd/dd where d is digit, and of those values none are dates, so they can be dropped.
        # The remaining dates all correspond to correct dates, i.e. the first two digits correspond to a value between 1-12.
        expiry_date_mask = df['expiry_date'].apply(lambda x : len(x) == 5 and x[2] == '/')
        df.loc[~expiry_date_mask, 'expiry_date'] = np.nan
        df.expiry_date = df.expiry_date.astype('string')

        # Clean card_provider
        # value_counts reveals that the invalid card providers all have less than 12 occurrences, so we just set those to np.nan
        card_provider_count = df['card_provider'].value_counts() # so we don't have to call value_coutns on this column multiple times
        valid_card_provider_mask = df['card_provider'].apply(lambda x: card_provider_count[x] > 12)
        df.loc[~valid_card_provider_mask, 'card_provider'] = np.nan
        # Finally, cast as string
        df.card_provider = df.card_provider.astype('string')
            
        # Clean date_payment_confirmed
        # A limited amount of entries are not valid dates.
        # Conveniently, all the valid dates can be parsed, and the rest throw a ValueError, hence we can reuse safe_parse
        df.date_payment_confirmed = df.date_payment_confirmed.apply(self.safe_parse)

        # Clean card_number
        # Entries with ?s return a valid credit card number after removing the ?s.
        df['card_number'] = df['card_number'].apply(lambda x: str(x).replace('?',''))
        card_number_mask = df['card_number'].apply(lambda x: bool(re.search(r'^\d+$', str(x))))
        df.loc[~card_number_mask, 'card_number'] = np.nan
        df.card_number = df.card_number.astype('string')

        # Now, we drop all NaN entries
        df.dropna(inplace = True)
        return df
    
    def clean_store_data(self, store_data_df: pd.DataFrame):
        '''
        Cleans the store data.

        Parameters
        ----------
        store_data_df : pd.DataFrame
            The store data to be cleaned.
            The dataframe must contain the columns: store_code, store_type, staff_numbers, locality, address, country_code, continent, longitude, latitude, opening_date.
            
        Returns
        -------
        df: pd.Dataframe
            The cleaned store data.
        '''

        df = store_data_df.copy()

        # the 'lat' column only has 11 non-null values, so we drop it. We check if the 'lat' column exists in case we run clean_store_data over a dataframe that already dropped it
        if 'lat' in df.columns:
            df.drop(columns=['lat'], inplace=True)
        # Clean store type
        # The valid store types all appear more than 4 times, except for 'Web Portal'.
        invalid_store_mask = df['store_type'].apply(lambda x: df['store_type'].value_counts()[x] < 4 and x != 'Web Portal')
        df.loc[invalid_store_mask, 'store_type'] = np.nan

        # We have to be careful with cleaning the remaining columns, as the Wep Portal has a lot of n/a values, but we don't want them to be dropped.
        # Since all stores with store types np.nan have otherwise invalid entries, we may as well drop them now, which facilitates the rest.
        # We will only drop the rows where the store_type is np.nan for now, to avoid accidentally dropping rows where a single value might be np.nan.
        df.dropna(subset = 'store_type', inplace=True)

        # Some of the staff numbers have letters in them. We can choose to either drop the entry, or assume that the numbers are correct without the letters, given that they fit within the range of the remaining entries
        # We opt for the latter.
        df['staff_numbers'] = df['staff_numbers'].apply(lambda x : re.sub('[^\d]', '', str(x)))

        # The dates are all fine and can be safely parsed.
        df['opening_date'] = df['opening_date'].apply(self.safe_parse)

        # Some of the continents contain the prefix 'ee' which we can drop.
        df['continent'] = df['continent'].apply(lambda x: x.replace('ee',''))

        # The columns 'address', 'longitude', 'locality', 'store_code', 'latitude', 'country_code' look fine.
        # So we finish by casting the entries to the right type, and resetting the index column.
        df.address, df.locality, df.store_code, df.store_type, df.country_code, df.continent = map(lambda x: x.astype('string'), (df.address, df.locality, df.store_code, df.store_type, df.country_code, df.continent))
        df.longitude, df.latitude = map(lambda x: pd.to_numeric(x, errors='coerce'), (df.longitude, df.latitude))
        df.staff_numbers = df.staff_numbers.astype('int')

        df.reset_index(drop = True, inplace=True)
        if 'index' in df.columns:
            df = df.drop(columns = ['index'])

        # We notice that longtitude appears in the 2nd column, and latitude in the 8th. These columns probably belong together.
        # At which point we might as well reorder all the columns in a more sensible order
        df = df[['store_code', 'store_type', 'staff_numbers', 'locality', 'address', 'country_code', 'continent', 'longitude', 'latitude', 'opening_date']]

        return df
    
    def convert_product_weights(self, product_data_df: pd.DataFrame):
        '''
        Converts the weights of all the products in a dataframe into kg and returns the converted dataframe.
        The accepted units are kg, g, ml, oz, and the method also accepts a single use of 'x' as a multiplier (e.g. 12x100g is converted to 1.2).

        Parameters
        ----------
        event_data_df : pd.DataFrame
            The dataframe containing the columns. The dataframe must contain a column named 'weight'.
            
        Returns
        -------
        df: pd.Dataframe
            The dataframe with all weights converted.
        '''
        
        df = product_data_df.copy()

        # The data contains the units kg, g, ml, and oz. (1 oz = 28.3495g)
        # some of the data contains entries such as '12 x 100g', which need to be converted as well.
        def single_conversion(entry: str):
            '''
            Converts a single weight entry into kg.
            The accepted units are kg, g, ml, oz, and the method also accepts a single use of 'x' as a multiplier (e.g. 12x100g is converted to 1.2).
            If the entry does not fit into that format, the method returns np.nan instead.
            
            Parameters
            ----------
            entry: str
                A string representing a weight.
            
            Return
            ------
            float:
                The weight in kg or np.nan if the entry was invalid.
            '''
            entry = str(entry).replace(' ','')
            if 'x' in entry:
                temp = entry.split('x')
                return float(temp[0]) * single_conversion(temp[1])
            elif 'kg' in entry:
                return float(entry.replace('kg',''))
            elif 'g' in entry:
                return float(entry.replace('g',''))/1000
            elif 'ml' in entry:
                return float(entry.replace('ml',''))/1000
            elif 'oz' in entry:
                return float(entry.replace('oz',''))*0.0283495
            else:
                return np.nan
        
        df['weight'] = df['weight'].apply(single_conversion)
        df['weight'] = pd.to_numeric(df.weight, errors='coerce')
        # df = df.rename(columns={'weight':'weight(kg)'}) # not a good idea because of how conflict with sql tables.
        return df
    
    def clean_products_data(self, product_data_df: pd.DataFrame):
        '''
        Cleans the product data.

        Parameters
        ----------
        product_card_df : pd.DataFrame
            The product data to be cleaned.
            The dataframe must contain the columns: product_name, product_price, weight, category, EAN, date_added, uuid, removed, product_code.
            
        Returns
        -------
        df: pd.Dataframe
            The cleaned product data.
        '''

        df = product_data_df.copy()
        df = self.convert_product_weights(df)

        # The column 'Unnamed:0' doesn't do anything.
        if 'Unnamed: 0' in df.columns:
            df.drop(columns=['Unnamed: 0'], inplace=True)

        # Clean product price
        df['product_price'] = df['product_price'].apply(lambda x: str(x).replace('£',''))
        df['product_price'] = pd.to_numeric(df.product_price, errors='coerce')
        # df = df.rename(columns={'product_price':'product_price(£)'})

        # Clean category
        # The invalid entries have all value_counts 1.
        category_count = df['category'].value_counts()
        df['category'] = df['category'].apply(lambda x: x if (x in category_count.keys() and category_count[x] > 1) else np.nan)

        # Clean EAN
        # We don't want to use to_numeric because some of the EANs are quite long and insufficient precision might cause a problem
        df['EAN'] = df['EAN'].apply(lambda x: x if not bool(re.search('[^\d]',str(x))) else np.nan)

        # Clean date_added
        # The valid dates can all be safely parsed
        df['date_added'] = df['date_added'].apply(self.safe_parse)

        # Clean uuid
        # uuids are of form xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx where x is alphanumerical
        valid_uuid_mask = df['uuid'].apply(lambda x: bool(re.search('[a-zA-Z\d]{8}-[a-zA-Z\d]{4}-[a-zA-Z\d]{4}-[a-zA-Z\d]{4}-[a-zA-Z\d]{12}', str(x))))
        df.loc[~valid_uuid_mask, 'uuid'] = np.nan

        # Clean removed
        df['removed'] = df['removed'].apply(lambda x: False if x == 'Removed' else (True if x == 'Still_avaliable' else np.nan))
        df = df.rename(columns={'removed':'still_available'})

        # Clean product code
        # All product codes have the form ld-X, where L is a letter, D a digit, and X a sequence of alphanumerical characters.
        valid_product_code_mask = df['product_code'].apply(lambda x: bool(re.search('[a-zA-Z]{1}[\d]{1}-[a-zA-Z\d]+', str(x))))
        df.loc[~valid_product_code_mask, 'product_code'] = np.nan

        # Now we drop all np.nans and cast the entries to the right types
        df.dropna(inplace=True)
        df.product_name, df.category, df.uuid, df.product_code, df.EAN = map(lambda x: x.astype('string'), (df.product_name, df.category, df.uuid, df.product_code, df.EAN))
        df.removed = df.removed.astype('bool')

        return df

    def clean_orders_data(self, order_data_df: pd.DataFrame):
        '''
        Cleans the orders data.

        Parameters
        ----------
        order_data_df : pd.DataFrame
            The order data to be cleaned.
            The dataframe must contain the columns: date_uuid, user_uuid, card_number, store_code, product_code, product_quantity.
            
        Returns
        -------
        df: pd.Dataframe
            The cleaned order data.
        '''
        df = order_data_df.copy()

        # Drop columns 'first_name', 'last_name', and '1'. Also drop 'index' and 'level_0'.
        df = df.drop(columns=['level_0','index','first_name','last_name','1'])

        # Every other column has nothing that needs to be dropped.
        # Cast to correct types
        df.date_uuid, df.user_uuid, df.store_code, df.product_code = map(lambda x: x.astype('string'), (df.date_uuid, df.user_uuid, df.store_code, df.product_code))
        return df
    
    def clean_events_data(self, event_data_df: pd.DataFrame):
        '''
        Cleans the events data.

        Parameters
        ----------
        event_data_df : pd.DataFrame
            The event data to be cleaned.
            The dataframe must contain the columns: timestamp, month, year, day, time_period, date_uuid.
            
        Returns
        -------
        df: pd.Dataframe
            The cleaned event data.
        '''

        df = event_data_df.copy()

        # Clean timestamp
        valid_time_stamp_mask = df['timestamp'].apply(lambda x: bool(re.search('[\d]{2}:[\d]{2}:[\d]{2}', str(x))))
        df.loc[~valid_time_stamp_mask, 'timestamp'] = np.nan

        # Clean day
        valid_day_mask = df['day'].apply(lambda x: bool(re.search('[\d]{1,2}', str(x))))
        df.loc[~valid_day_mask, 'day'] = np.nan

        # Clean month
        valid_month_mask = df['month'].apply(lambda x: bool(re.search('[\d]{1,2}', str(x))))
        df.loc[~valid_month_mask, 'month'] = np.nan

        # Clean year
        valid_year_mask = df['year'].apply(lambda x: bool(re.search('[\d]{4}', str(x))))
        df.loc[~valid_year_mask, 'year'] = np.nan

        # Clean time_period
        time_period = df['time_period'].value_counts()
        df['time_period'] = df['time_period'].apply(lambda x: x if (x in time_period.keys() and time_period[x] > 15) else np.nan)

        # Clean date_uuid
        valid_date_uuid_mask = df['date_uuid'].apply(lambda x: bool(re.search('[a-zA-Z\d]{8}-[a-zA-Z\d]{4}-[a-zA-Z\d]{4}-[a-zA-Z\d]{4}-[a-zA-Z\d]{12}', str(x))))
        df.loc[~valid_date_uuid_mask, 'date_uuid'] = np.nan

        # Drop np.nan's, cast to correct types, and reset index
        df.dropna(inplace = True)
        df.month, df.year, df.day = map(lambda x: x.astype('int'), (df.month, df.year, df.day))
        df.time_period, df.date_uuid = map(lambda x: x.astype('string'), (df.time_period, df.date_uuid))
        df.timestamp, df.time_period, df.date_uuid = map(lambda x: x.astype('string'), (df.timestamp, df.time_period, df.date_uuid))
        df.reset_index(drop = True, inplace=True)

        # Then add a date column combining year-month-day timestamp, for convenience
        df['date'] = (df['year'].astype('string') + '-' + df['month'].astype('string') + '-' + df['day'].astype('string') + ' ' + df['timestamp']).apply(parse)

        return df

    @staticmethod
    def safe_parse(date_string: str):
        '''
        Safely parses a date avoiding ValueError. If the string contains a valid date, it will return it as a parsed date. Otherwise, if
        will return np.nan (instead of throwing a ValueError)

        Parameters
        ----------
        date_string: str
            The string containing a candidate for a date to be parsed.
        
        Returns
        -------
            The parsed date if the string contain a valid date, or np.nan otherwise.
        '''
        try:
            date = parse(str(date_string))
            return date
        except ValueError:
            return np.nan
        