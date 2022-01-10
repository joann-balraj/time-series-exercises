# Time Series: Data Acquisition

########## Exercises ##########

# Exercise 1: Using the code from the lesson as a guide
# and the REST API from https://python.zgulde.net/api/v1/items as we did in the lesson, 
# create a dataframe named items that has all of the data for items.


def get_items():

    """
    This function acquires data from a REST API at the url above and returns a dataframe containing all the items
    """

    import pandas as pd
    import requests  

    # define base url to obtain api from
    url_items = 'https://python.zgulde.net/api/v1/items'

    # create response containing the contents of the response from the api
    response_items = requests.get(url_items)

    # turn that .json content into a dictionary for use with Python
    data_items = response_items.json()

    # create a dataframe containing the dictionary created from the .json sent by the api
    items = pd.DataFrame(data_items['payload']['items'])

    return items


# Exercise 2: Do the same thing as #1, but for stores (https://python.zgulde.net/api/v1/stores)

def get_stores():

    """
        This function acquires data from a REST API at the url above and returns a dataframe containing all the stores
    """

    import pandas as pd
    import requests  

    # Define base url to obtain api from
    stores_url = 'https://python.zgulde.net/api/v1/stores'

    # create response containing the stores from the api
    response_stores = requests.get(stores_url)

    #Turn that .json content into a dictionary for use with Python
    data_stores = response_stores.json()

    #Create a dataframe containing the dictionary created from the .json sent by the api
    df_stores = pd.DataFrame(data_stores['payload']['stores'])

    return df_stores



# Exercise 3: 
# 3. Extract the data for sales (https://python.zgulde.net/api/v1/sales). 
# There are a lot of pages of data here, so your code will need to be a little more complex. 
# Your code should continue fetching data from the next page until all of the data is extracted.


def get_sales():

    """
    This function acquires data from a REST API at the url above and returns a dataframe containing all the sales
    """

    import pandas as pd
    import requests  

    # define base url to obtain api from
    sales_url= 'https://python.zgulde.net/api/v1/sales'

    # iterating thru every page and concatenating the sales info from each page, we create a loop

    # acquire .json from url
    response_sales = requests.get(sales_url)

    # turn .json content into dictionary
    data_sales = response_sales.json()

    # turn dictionary into a dataframe
    df_sales = pd.DataFrame(data_sales['payload']['sales'])

    # getting ready to iterate through all pages 
    num_pages = data_sales['payload']['max_page']

    # loop through the iterations
    for i in range(1,num_pages):

        response_sales = requests.get(sales_url)
        data_sales = response_sales.json()
        df_sales = pd.concat([df_sales, pd.DataFrame(data_sales['payload']['sales'])])

    return df_sales 

# Exercise 4: Save the data in your files to local csv files so that it will be faster to access in the future.

def create_sales_data_csv():

    """
    This function calls the get_sales function and creates a .csv with sales data and saves it locally
    """

    df_sales = get_sales()

    # create a csv from sales data and store locally
    df_sales.to_csv('sales.csv')


# Exercise 5: Combine the data from your three separate dataframes into one large dataframe.

def combine_sales_stores_items_data():
    
    """
    This function calls 3 functions that get sales, stores, and items and concatenates and returns all the data in one dataframe
    """

    import pandas as pd

    df_sales = get_sales()
    stores = get_stores()
    items = get_items()

    df_sales_and_stores = pd.merge(df_sales, df_stores, how='left', left_on='store' , right_on='store_id')

    df_all = pd.merge(df_sales_and_stores, df_items, how='left', left_on='item', right_on='item_id')

    return df_all


# Exercise 6: Acquire the Open Power Systems Data for Germany, which has been rapidly expanding its renewable energy production in recent years. 
# The data set includes country-wide totals of electricity consumption, wind power production, and solar power production for 2006-2017. 
# You can get the data here: https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv

def csv_to_df(url):

    """
    This function reads data from a link to a .csv file and returns a dataframe
    """

    import pandas as pd

    df_from_csv = pd.read_csv(url)

    return df_from_csv