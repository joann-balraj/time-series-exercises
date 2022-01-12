# Data Prep with Time Series Exercises


import acquire
import pandas as pd


######### Store Items Data #########

#Acquire store data from acquire.py function
#============================================

df = acquire.combine_sales_stores_items_data()

# 1. Convert date column to datetime format.
#====================================================

def convert_col_to_date():

    df.sale_date = pd.to_datetime(df.sale_date)

    return 


# 2. Plot the distribution of sale_amount and item_price.
#=================================================================

def plot_dists():

    df.sale_amount.hist()
    df.item_price.hist()
    return


# 3. Set the index to be the datetime variable.
#=======================================================

def set_datevar_to_index(df):

    df = df.set_index('sale_date').sort_index()

    return


# 4. Add a 'month' and 'day of week' column to your dataframe.
#======================================================================

def add_month_dayofweek_col():

    df['month'] = df.index.month
    df['day_of_week'] = df.index.day_of_week
    return



# 5. Add a column to your dataframe, sales_total, which is a derived from sale_amount (total items) and item_price.
#===========================================================================================================================

def add_sales_total_col():

    df['sales_total'] = df.sale_amount * df.item_price
    return



########## OPS Data ################

# Function to read the OPS data acquired in the Acquire exercises opsd_germany_daily.csv
# ==================================

def read_germany_energy_data():
    df_germany = pd.read_csv('https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv')

    return  df_germany

# 1. Convert date column to datetime format.
#====================================================

def convert_G_data_to_date():
    df_germany = read_germany_energy_data()
    df_germany.Date = pd.to_datetime(df_germany.Date)

    return


# 2. Plot the distribution of variables
#=================================================================
def plot_G_data_dist():
    df_germany = read_germany_energy_data()
    df_germany.Consumption.hist()
    df_germany.Wind.hist()
    df_germany.Solar.hist()

    #Rename Wind+Solar column to Wind_Solar
    df_germany.rename(columns={"Wind+Solar": "Wind_Solar"}, inplace=True)
    df_germany.Wind_Solar.hist()

    return

# 3. Set the index to be the datetime variable.
#=======================================================
def set_G_data_to_index():    
    df_germany = read_germany_energy_data()
    df_germany = df_germany.set_index('Date').sort_index()

    return

# 4. Add a 'month' column to your dataframe.
#===================================================
def add_month_col_to_G_data():
    df_germany = read_germany_energy_data()
    df_germany['month'] = df_germany.index.month  

    return

# 5. Fill any missing values.
#=====================================

def fill_missing_vals():
    from sklearn.impute import SimpleImputer
    imputer = SimpleImputer(strategy='most_frequent')

    df_germany = read_germany_energy_data()

    for col in df_germany.columns:
        df_germany[[col]] = imputer.fit_transform(df_germany[[col]])

    return