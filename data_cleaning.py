import pandas as pd
import numpy as np
import re
import yaml
from dateutil.parser import parse
from datetime import datetime


class DataCleaning:
    
    # cleans the data from db_creds.yaml
    def clean_user_data(self, user_data_df: pd.DataFrame):

        df = user_data_df.copy()

        if 'index' in df.columns:
            df = df.set_index('index')

        # apply .astype('string') to several columns
        df.last_name, df.company, df.email_address, df.address, df.country, df.country_code = map(lambda x: x.astype('string'), (df.last_name, df.company, df.email_address, df.address, df.country, df.country_code))

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

        df = card_data_df.copy()

        # Clean expiry_date
        # A limited amount of entries are not of the format dd/dd where d is digit, and of those values none are dates, so they can be dropped.
        # The remaining dates all correspond to correct dates, i.e. the first two digits correspond to a value between 1-12.
        # We can either keep the formay %m/%y, or the dtype datetime64. We opt for the latter.
        def safe_expiry_date_conversion(date, format='%m/%y'):
            try:
                date = datetime.strptime(date, format)
                return date
            except ValueError:
                return np.nan
        df.expiry_date = df.expiry_date.apply(safe_expiry_date_conversion)

        # Clean card_provider
        # value_counts reveals that the invalid card providers all have less than 12 occurrences, so we just set those to np.nan
        card_provider_count = df['card_provider'].value_counts() # so we don't have to call value_coutns on this column multiple times
        mask = df['card_provider'].apply(lambda x: card_provider_count[x] > 12)
        df.loc[~mask, 'card_provider'] = np.nan
        # Finally, cast as string
        df.card_provider = df.card_provider.astype('string')
            
        # Clean date_payment_confirmed
        # A limited amount of entries are not valid dates.
        # Conveniently, all the valid dates can be parsed, and the rest throw a ValueError, hence we can reuse safe_parse
        df.date_payment_confirmed = df.date_payment_confirmed.apply(self.safe_parse)

        # Clean card_number
        # Some entries have ?s than when removed return a valid credit card number.
        # For each provider, we check for number length, as some lenghts deviate from the others of the same provider.
        # We only accept lengths that occur over 50 times. The acceptable lengths are stored in 'bank_card_number_lengths.yaml'.
        # We can't just use pd.to_numeric because some codes are 19 digits long, which exceeds the precision of float64 numbers and thus changes some card numbers.
        with open('bank_card_number_lengths.yaml', 'r') as stream:
            accepted_card_number_lengths = yaml.safe_load(stream)
        bank_card_names = list(accepted_card_number_lengths.keys())

        question_mark_replacer = lambda x: str(x).replace('?','')
        df['card_number'] = df['card_number'].apply(question_mark_replacer)

        for name in bank_card_names:
            card_name_mask = df['card_provider'] == name
            df_card_by_name = df[card_name_mask]

            has_correct_length = lambda x : len(str(x)) in accepted_card_number_lengths[name]
            is_numeric = lambda x : bool(re.search(r'^\d+$', str(x)))

            df.loc[card_name_mask, 'card_number'] = df.loc[card_name_mask, 'card_number'].apply(lambda x: x if has_correct_length(x) and is_numeric(x) else np.nan)
        
        # Now, we drop all NaN entries
        df.dropna(inplace = True)
        # And we can now cast card_number to an int
        df.card_number = df.card_number.astype('int64') # needs to specify int64, otherwise it automatically chooses int32, which isn't enough
        return df
    
    def clean_store_data(self, store_data_df: pd.DataFrame):

        df = store_data_df.copy()

        # the 'lat' column only has 11 non-null values, so we drop it. We check if the 'lat' column exists in case we run clean_store_data over a dataframe that already dropped it
        if 'lat' in df.columns:
            df.drop(columns=['lat'], inplace=True)
        # Clean store type
        # The valid store types all appear more than 4 times, except for 'Web Portal'.
        mask = df['store_type'].apply(lambda x: df['store_type'].value_counts()[x] < 4 and x != 'Web Portal')
        df.loc[mask, 'store_type'] = np.nan

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
        df = product_data_df.copy()

        # The data contains the units kg, g, ml, and oz. (1 oz = 28.3495g)
        # some of the data contains entries such as '12 x 100g', which need to be converted as well.
        def single_conversion(entry: str):
            entry = str(entry).replace(' ','')
            if 'x' in entry:
                temp = entry.split('x')
                return float(temp[0]) * single_conversion(temp[1])
            elif 'kg' in entry:
                return float(entry.replace('kg',''))
            elif 'g' in entry:
                return float(entry.replace('g',''))
            elif 'ml' in entry:
                return entry.replace('ml','')
            elif 'oz' in entry:
                return entry.replace('oz','')
            else:
                return np.nan
        
        df['weight'] = df['weight'].apply(single_conversion)
        df['weight'] = pd.to_numeric(df.weight, errors='coerce')
        df = df.rename(columns={'weight':'weight(kg)'})
        return df
    
    def clean_products_data(self, product_data_df: pd.DataFrame):
        df = product_data_df.copy()
        df = self.convert_product_weights(df)

        # The column 'Unnamed:0' doesn't do anything.
        if 'Unnamed: 0' in df.columns:
            df.drop(columns=['Unnamed: 0'], inplace=True)

        # Clean product price
        df['product_price'] = df['product_price'].apply(lambda x: str(x).replace('£',''))
        df['product_price'] = pd.to_numeric(df.product_price, errors='coerce')
        df = df.rename(columns={'product_price':'product_price(£)'})

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
        mask = df['uuid'].apply(lambda x: bool(re.search('[a-zA-Z\d]{8}-[a-zA-Z\d]{4}-[a-zA-Z\d]{4}-[a-zA-Z\d]{4}-[a-zA-Z\d]{12}', str(x))))
        df.loc[~mask, 'uuid'] = np.nan

        # Clean removed
        df['removed'] = df['removed'].apply(lambda x: True if x == 'Removed' else (False if x == 'Still_avaliable' else np.nan))

        # Clean product code
        # All product codes have the form ld-X, where L is a letter, D a digit, and X a sequence of alphanumerical characters.
        mask = df['product_code'].apply(lambda x: bool(re.search('[a-zA-Z]{1}[\d]{1}-[a-zA-Z\d]+', str(x))))
        df.loc[~mask, 'product_code'] = np.nan

        # Now we drop all np.nans and cast the entries to the right types
        df.dropna(inplace=True)
        df.product_name, df.category, df.uuid, df.product_code = map(lambda x: x.astype('string'), (df.product_name, df.category, df.uuid, df.product_code))
        df.EAN = df.EAN.astype('int64')
        df.removed = df.removed.astype('bool')

        return df

    def clean_orders_data(self, order_data_df: pd.DataFrame):
        df = order_data_df.copy()

        # Drop columns 'first_name', 'last_name', and '1'. Also drop 'index' and 'level_0'.
        df = df.drop(columns=['level_0','index','first_name','last_name','1'])

        # Every other column has nothing that needs to be dropped.
        # Cast to correct types
        df.date_uuid, df.user_uuid, df.store_code, df.product_code = map(lambda x: x.astype('string'), (df.date_uuid, df.user_uuid, df.store_code, df.product_code))
        return df
    
    def clean_events_data(self, event_data_df: pd.DataFrame):
        df = event_data_df.copy()

        # Clean timestamp
        mask = df['timestamp'].apply(lambda x: bool(re.search('[\d]{2}:[\d]{2}:[\d]{2}', str(x))))
        df.loc[~mask, 'timestamp'] = np.nan

        # Clean day
        mask = df['day'].apply(lambda x: bool(re.search('[\d]{1,2}', str(x))))
        df.loc[~mask, 'day'] = np.nan

        # Clean month
        mask = df['month'].apply(lambda x: bool(re.search('[\d]{1,2}', str(x))))
        df.loc[~mask, 'month'] = np.nan

        # Clean year
        mask = df['year'].apply(lambda x: bool(re.search('[\d]{4}', str(x))))
        df.loc[~mask, 'year'] = np.nan

        # Clean time_period
        time_period = df['time_period'].value_counts()
        df['time_period'] = df['time_period'].apply(lambda x: x if (x in time_period.keys() and time_period[x] > 15) else np.nan)

        # Clean date_uuid
        mask = df['date_uuid'].apply(lambda x: bool(re.search('[a-zA-Z\d]{8}-[a-zA-Z\d]{4}-[a-zA-Z\d]{4}-[a-zA-Z\d]{4}-[a-zA-Z\d]{12}', str(x))))
        df.loc[~mask, 'date_uuid'] = np.nan

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
    def safe_parse(date_string):
            try:
                date = parse(str(date_string))
                return date
            except ValueError:
                return np.nan
        