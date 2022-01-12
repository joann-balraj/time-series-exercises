import requests
import pandas as pd
import os

def new_retail_data(base_url='https://python.zgulde.net'):
    '''
    This function acquires new retail data, returns three dataframes, and saves those dataframes to .csv files.
    '''
    # Acquiring items data
    response = requests.get('https://python.zgulde.net/api/v1/items')
    data = response.json()
    df = pd.DataFrame(data['payload']['items'])
    while data['payload']['next_page'] != None:
        response = requests.get(base_url + data['payload']['next_page'])
        data = response.json()
        df = pd.concat([df, pd.DataFrame(data['payload']['items'])]).reset_index(drop=True)
    items_df = df.copy()
    print("Items data acquired...")

    # Acquiring stores data
    response = requests.get('https://python.zgulde.net/api/v1/stores')
    data = response.json()
    df = pd.DataFrame(data['payload']['stores'])
    stores_df = df.copy()
    print("Stores data acquired...")

    # Acquiring sales data
    response = requests.get('https://python.zgulde.net/api/v1/sales')
    data = response.json()
    df = pd.DataFrame(data['payload']['sales'])
    while data['payload']['next_page'] != None:
        response = requests.get(base_url + data['payload']['next_page'])
        data = response.json()
        df = pd.concat([df, pd.DataFrame(data['payload']['sales'])]).reset_index(drop=True)
    sales_df = df.copy()
    print("Sales data acquired")

    # Saving new data to .csv files
    items_df.to_csv("items.csv", index=False)
    stores_df.to_csv("stores.csv", index=False)
    sales_df.to_csv("sales.csv", index=False)
    print("Saving data to .csv files")

    return items_df, stores_df, sales_df

def get_store_data():
    '''
    This function reads in retail data from the website if there are no csv files to pull from
    '''
    # Checks if .csv files are present. If any are missing, will acquire new data for all three datasets
    if (os.path.isfile('items.csv') == False) or (os.path.isfile('sales.csv') == False) or (os.path.isfile('stores.csv') == False):
        print("Data is not cached. Acquiring new data...")
        items_df, stores_df, sales_df = new_retail_data()
    else:
        print("Data is cached. Reading from .csv files")
        items_df = pd.read_csv('items.csv')
        print("Items data acquired...")
        stores_df = pd.read_csv('stores.csv')
        print("Stores data acquired...")
        sales_df = pd.read_csv('sales.csv')
        print("Sales data acquired...")

    combined_df = sales_df.merge(items_df, how='left', left_on='item', right_on='item_id').drop(columns=['item'])
    combined_df = combined_df.merge(stores_df, how='left', left_on='store', right_on='store_id').drop(columns=['store'])
    print("Acquisition complete")
    return combined_df

def new_power_data():
    opsd = pd.read_csv("https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv")
    opsd = opsd.fillna(0)
    print("Saving data to .csv file")
    opsd.to_csv('opsd_germany_daily_data.csv', index=False)
    return opsd

def get_power_data():
    if os.path.isfile('opsd_germany_daily_data.csv') == False:
        print("Data is not cached. Acquiring new power data.")
        opsd = new_power_data()
    else:
        print("Data is cached. Reading data from .csv file.")
        opsd = pd.read_csv('opsd_germany_daily_data.csv')
    print("Acquisition complete")
    return opsd





def get_items():

    import pandas as pd
    import requests  

    # Define base url to obtain api from
    url= 'https://python.zgulde.net'

    # create response containing the contents of the response from the api
    response = requests.get(url + '/api/v1/items')

    #Turn that .json content into a dictionary for use with Python
    data = response.json()

    #Create a dataframe containing the dictionary created from the .json sent by the api
    df_items = pd.DataFrame(data['payload']['items'])

    return df_items


#Function for Exercise #2
#------------------------
# 2. Do the same thing as #1, but for stores (https://python.zgulde.net/api/v1/stores)

#This function acquires data from a REST API at the url above and returns a dataframe containing all the stores

def get_stores():

    import pandas as pd
    import requests  

    # Define base url to obtain api from
    url= 'https://python.zgulde.net'

    # create response containing the stores from the api
    response_stores = requests.get(url + '/api/v1/stores')

    #Turn that .json content into a dictionary for use with Python
    data_stores = response_stores.json()

    #Create a dataframe containing the dictionary created from the .json sent by the api
    df_stores = pd.DataFrame(data_stores['payload']['stores'])

    return df_stores

#Function for Exercise #3
#------------------------
# 2. Extract the data for sales (https://python.zgulde.net/api/v1/sales). 
# There are a lot of pages of data here, so your code will need to be a little more complex. 
# Your code should continue fetching data from the next page until all of the data is extracted. 

#This function acquires data from a REST API at the url above and returns a dataframe containing all the sales

def get_sales():

    import pandas as pd
    import requests  

    # Define base url to obtain api from
    url= 'https://python.zgulde.net'

    #Iterating thru every page and concatenating the sales info from each page, we create a loop

    #acquire .json from url
    response_sales = requests.get(url + '/api/v1/sales')

    #turn .json content into dictionary
    data_sales = response_sales.json()

    #turn dictionary into a dataframe
    df_sales = pd.DataFrame(data_sales['payload']['sales'])

    #Get ready to iterate thru all pages 
    num_pages = data_sales['payload']['max_page']

    # loop thru the iterations
    for i in range(1,num_pages):

        response_sales = requests.get(url + data_sales['payload']['next_page'])
        data_sales = response_sales.json()
        df_sales = pd.concat([df_sales, pd.DataFrame(data_sales['payload']['sales'])])

    return df_sales    
 

#Function for Exercise #4
#------------------------
#4. Save the data in your files to local csv files so that it will be faster to access in the future.

#This function calls the get_sales function and creates a .csv with sales data and saves it locally
def create_sales_data_csv():

    df_sales = get_sales()

    #create a csv from sales data and store locally
    df_sales.to_csv('sales.csv')


#Function for Exercise #5
#------------------------
# Combine the data from your three separate dataframes into one large dataframe.

#This function calls 3 functions that get sales, stores, and items and concatenates and returns all the data in one dataframe

def combine_sales_stores_items_data():

    import pandas as pd

    df_sales = get_sales()
    df_stores = get_stores()
    df_items = get_items()

    df_sales_and_stores = pd.merge(df_sales, df_stores, how='left', left_on='store' , right_on='store_id')

    df_all = pd.merge(df_sales_and_stores, df_items, how='left', left_on='item', right_on='item_id')

    return df_all


#Function for Exercise #6
#------------------------
#This function reads data from a link to a .csv file and returns a dataframe

def csv_to_df(url):

    import pandas as pd

    df_from_csv = pd.read_csv(url)

    return df_from_csv