# Data pipeline automation script to process data from MySQL and deploy to GDS

from sqlalchemy import create_engine
import pymysql

engine = sqlalchemy.create_engine('mysql+pymysql://root:password@localhost:3306/test_schema').connect()

import pandas as pd
import time

sales = pd.read_sql_table("gds_sale_transactions", engine)
purchasing = pd.read_sql_table("gds_purchase_transactions", engine)
mach_rent = pd.read_sql_table("gds_machine_rental_transactions", engine)
mach_pur = pd.read_sql_table("gds_machine_purchase_transactions", engine)
expenses = pd.read_sql_table("gds_expense_transactions", engine)
processing = pd.read_sql_table("gds_processing_transactions", engine)

product_legends_df = pd.read_excel('Products Information.xlsx')

def product_id_lookup(prod, category):
    prod_row = product_legends_df[(product_legends_df['Product Name'] == prod) & (product_legends_df['Product Category'] == category)]
    if len(prod_row) == 0:
    # print(prod)
    # print(category)
    # print('Error')
        return 'Unknown'
    product_id = prod_row['SL'].values[0]
    return product_id

def product_id_lookup_combined(combined):
    product = combined.split(':')[0] 
    cat = combined.split(':')[1]
    product_id = product_id_lookup(product, cat)
    return product_id 

hub_df = pd.read_sql_table("gds_users_information", engine)

def hub_info_lookup(user_id):
    region = 'Unknown'
    franchisee = 'Unknown'
    region_row = hub_df[hub_df['user_id'] == user_id]
    if len(region_row) > 0:
        region = region_row.iloc[0]['region']
        franchisee = region_row.iloc[0]['parent_franchisee']
    combined = region + ":" + franchisee
    return combined

def region_lookup(combined):
    region = combined.split(':')[0] 
    return region

def franchisee_lookup(combined):
    franchisee = combined.split(':')[1]
    return franchisee 

def trans_type_lookup(cat):
    trans = 'Unknown'
    if cat == 'Crop':
        trans = 'Crop Buying and Selling'
    elif cat == 'Machinery':
        trans = 'Machinery Buying Selling'
    else:
        trans = 'Agri Inputs Selling'
    return trans


country = 'Bangladesh'
cols =  ['Transaction ID', 'User ID', 'User', 'User Type', 'Franchisee', 'Transaction Type',
       'Transaction Type Level 2', 'Date of Transaction',
       'Customer Name', 'Phone Number', 'Customer ID', 'Market Type',
       'Product ID', 'Product', 'Product Category', 'Quantity',
       'Unit Type', 'Unit Price', 'Product Amount', 'Paid Amount', 
        'Currency Rate', 'Paid Amount USD', 'Country', 'Region', 'Revenue', 'COGS', 'Net Profit']

# ____________________________________________________________________________________
# sales
new_df = pd.DataFrame(columns = cols)

# transaction features
tic = time.time()

new_df['Transaction ID'] = sales['transaction_id']
new_df['Country'] = country
new_df['Transaction Type Level 2'] = 'Sale'
new_df['Date of Transaction'] = sales['transaction_date']

new_df['Transaction Type'] = sales['category'].apply(trans_type_lookup)

# Product
new_df['Product'] = sales['product']
new_df['Product Category'] = sales['category']
products_comb_df = pd.DataFrame()
products_comb_df['Combined'] = new_df['Product'] + ":" + new_df['Product Category']
products_comb_df['ID'] = products_comb_df['Combined'].apply(product_id_lookup_combined)
new_df['Product ID'] = products_comb_df['ID']

print('Here')

# customer info
new_df['Customer ID'] = sales['customer_id']
new_df['Customer Name'] = sales['customer_name']
new_df['Phone Number'] = sales['customer_mobile']
new_df['Market Type'] = sales['market_type']

# basic info
new_df['Quantity'] = sales['quantity']
new_df['Unit Type'] = sales['unit_type']
new_df['Unit Price'] = sales['unit_price']
new_df['Paid Amount'] = sales['paid_amount']  # paid_amount missing
new_df['Product Amount'] = sales['product_amount']

# hub info
hub_comb_df = pd.DataFrame()
new_df['User ID'] = sales['user_id']
new_df['User'] = sales['user_name']
new_df['User Type'] = sales['user_type']

# region and franchisee
hub_comb_df['User ID'] = new_df['User ID']
hub_comb_df['Combined'] = new_df['User ID'].apply(hub_info_lookup)
hub_comb_df['Region']= hub_comb_df['Combined'].apply(region_lookup)
hub_comb_df['Franchisee']= hub_comb_df['Combined'].apply(franchisee_lookup)
new_df['Region'] = hub_comb_df['Region']
new_df['Franchisee'] = hub_comb_df['Franchisee']

# usd numbers
new_df['Currency Rate'] = 1/sales["currency_exchange_rate"]
new_df['Paid Amount USD'] = new_df['Paid Amount'].astype(float) * new_df['Currency Rate']

toc = time.time()
print(toc - tic, ' seconds')

sales_flagged = pd.DataFrame.copy(new_df)

# __________________________________________________________________________________________
# purchase

new_df = pd.DataFrame(columns = cols)
tic = time.time()

# transaction features
new_df['Transaction ID'] = purchasing['transaction_id']
new_df['Country'] = country
new_df['Transaction Type Level 2'] = 'Purchase'
new_df['Date of Transaction'] = purchasing['transaction_date']

new_df['Transaction Type'] = purchasing['category'].apply(trans_type_lookup)

# Product
new_df['Product'] = purchasing['product']
new_df['Product Category'] = purchasing['category']
products_comb_df = pd.DataFrame()
products_comb_df['Combined'] = new_df['Product'] + ":" + new_df['Product Category']
products_comb_df['ID'] = products_comb_df['Combined'].apply(product_id_lookup_combined)
new_df['Product ID'] = products_comb_df['ID']

# customer info
new_df['Customer ID'] = purchasing['supplier_id']
new_df['Customer Name'] = purchasing['supplier_name']
new_df['Phone Number'] = purchasing['supplier_mobile']
new_df['Market Type'] = purchasing['market_type']

# basic info
new_df['Quantity'] = purchasing['quantity']
new_df['Unit Type'] = purchasing['unit_type']
new_df['Unit Price'] = purchasing['unit_price']
new_df['Paid Amount'] = purchasing['paid_amount']
new_df['Product Amount'] = sales['product_amount']

# hub info
hub_comb_df = pd.DataFrame()
new_df['User ID'] = purchasing['user_id']
new_df['User'] = purchasing['user_name']
new_df['User Type'] = purchasing['user_type']
# region and franchisee
hub_comb_df['User ID'] = new_df['User ID']
hub_comb_df['Combined'] = new_df['User ID'].apply(hub_info_lookup)
hub_comb_df['Region']= hub_comb_df['Combined'].apply(region_lookup)
hub_comb_df['Franchisee']= hub_comb_df['Combined'].apply(franchisee_lookup)
new_df['Region'] = hub_comb_df['Region']
new_df['Franchisee'] = hub_comb_df['Franchisee']

# usd numbers
new_df['Currency Rate'] = 1/purchasing["currency_exchange_rate"]
new_df['Paid Amount USD'] = new_df['Paid Amount'].astype(float) * new_df['Currency Rate']


toc = time.time()
print(toc - tic, ' seconds')

purchasing_flagged = pd.DataFrame.copy(new_df)

# ________________________________________________________________________________________
# machine rent

new_df = pd.DataFrame(columns = cols)

tic = time.time()

# transaction features
new_df['Transaction ID'] = mach_rent['transaction_id']
new_df['Country'] = country
new_df['Transaction Type Level 2'] = 'Machinery Rental'
new_df['Date of Transaction'] = mach_rent['transaction_date']

new_df['Transaction Type'] = 'Farm Machinery Rental'

# Product
new_df['Product'] = mach_rent['product']
new_df['Product Category'] = mach_rent['category']
products_comb_df = pd.DataFrame()
products_comb_df['Combined'] = new_df['Product'] + ":" + new_df['Product Category']
products_comb_df['ID'] = products_comb_df['Combined'].apply(product_id_lookup_combined)
new_df['Product ID'] = products_comb_df['ID']

# customer info
new_df['Customer ID'] = mach_rent['customer_id']
new_df['Customer Name'] = mach_rent['customer_name']
new_df['Phone Number'] = mach_rent['customer_mobile']
new_df['Market Type'] = 'Farmer'

# basic info
new_df['Quantity'] = mach_rent['quantity']
new_df['Unit Type'] = mach_rent['unit_type']
new_df['Unit Price'] = mach_rent['unit_price']
new_df['Paid Amount'] = mach_rent['paid_amount']
new_df['Product Amount'] = mach_rent['sub_total_amount']

# hub info
hub_comb_df = pd.DataFrame()
new_df['User ID'] = mach_rent['user_id']
new_df['User'] = mach_rent['user_name']
new_df['User Type'] = mach_rent['user_type']
# region and franchisee
hub_comb_df['User ID'] = new_df['User ID']
hub_comb_df['Combined'] = new_df['User ID'].apply(hub_info_lookup)
hub_comb_df['Region']= hub_comb_df['Combined'].apply(region_lookup)
hub_comb_df['Franchisee']= hub_comb_df['Combined'].apply(franchisee_lookup)
new_df['Region'] = hub_comb_df['Region']
new_df['Franchisee'] = hub_comb_df['Franchisee']

# usd numbers
new_df['Currency Rate'] = 1/mach_rent["currency_exchange_rate"]
new_df['Paid Amount USD'] = new_df['Paid Amount'] * new_df['Currency Rate']

toc = time.time()
print(toc - tic, ' seconds')

machine_rent_flagged = pd.DataFrame.copy(new_df)

# ____________________________________________________________________________________________
# machine purchase

new_df = pd.DataFrame(columns = cols)
tic = time.time()

# transaction features
new_df['Transaction ID'] = mach_pur['transaction_id']
new_df['Country'] = country
new_df['Transaction Type Level 2'] = 'Machinery'
new_df['Date of Transaction'] = mach_pur['transaction_date']

new_df['Transaction Type'] = 'Machinery Buying Selling'

# Product
new_df['Product'] = mach_pur['product']
new_df['Product Category'] = mach_pur['category']
products_comb_df = pd.DataFrame()
products_comb_df['Combined'] = new_df['Product'] + ":" + new_df['Product Category']
products_comb_df['ID'] = products_comb_df['Combined'].apply(product_id_lookup_combined)
new_df['Product ID'] = products_comb_df['ID']

# customer info
new_df['Customer ID'] = mach_pur['supplier_id']
new_df['Customer Name'] = mach_pur['supplier_name']
new_df['Phone Number'] = mach_pur['supplier_mobile']
new_df['Market Type'] = 'Farmer'

# basic info
new_df['Quantity'] = mach_pur['quantity']
# new_df['Unit Type'] = mach_r['unit_type']
new_df['Unit Price'] = mach_pur['unit_price']
new_df['Paid Amount'] = mach_pur['paid_amount'] # REZA: paid_amount
new_df['Product Amount'] = mach_pur['paid_amount'] # REZA: paid_amount

# hub info
hub_comb_df = pd.DataFrame()
new_df['User ID'] = mach_pur['user_id']
new_df['User'] = mach_pur['user_name']
new_df['User Type'] = mach_pur['user_type']
# region and franchisee
hub_comb_df['User ID'] = new_df['User ID']
hub_comb_df['Combined'] = new_df['User ID'].apply(hub_info_lookup)
hub_comb_df['Region']= hub_comb_df['Combined'].apply(region_lookup)
hub_comb_df['Franchisee']= hub_comb_df['Combined'].apply(franchisee_lookup)
new_df['Region'] = hub_comb_df['Region']
new_df['Franchisee'] = hub_comb_df['Franchisee']

# usd numbers
new_df['Currency Rate'] = 1/mach_pur["currency_exchange_rate"]
new_df['Paid Amount USD'] = new_df['Paid Amount'].astype(float) * new_df['Currency Rate']


toc = time.time()
print(toc - tic, ' seconds')

mach_pur_flagged = pd.DataFrame.copy(new_df)

# processing
new_df = pd.DataFrame(columns = cols)

# transaction features
tic = time.time()

new_df['Transaction ID'] = processing['transaction_id']
new_df['Country'] = 'Bangladesh'
new_df['Transaction Type Level 2'] = 'Processing'
new_df['Date of Transaction'] = processing['transaction_date']

new_df['Transaction Type'] = 'Agri Inputs Selling'

# Product
new_df['Product'] = processing['product']
new_df['Product Category'] = processing['category']
products_comb_df = pd.DataFrame()
products_comb_df['Combined'] = new_df['Product'] + ":" + new_df['Product Category']
products_comb_df['ID'] = products_comb_df['Combined'].apply(product_id_lookup_combined)
new_df['Product ID'] = products_comb_df['ID']

print('Here')

# customer info
# new_df['Customer ID'] = sales['customer_id']
# new_df['Customer Name'] = sales['customer_name']
# new_df['Phone Number'] = sales['customer_mobile']
new_df['Market Type'] = "Farmers' Hub"

# basic info
new_df['Quantity'] = processing['quantity']
new_df['Unit Type'] = processing['unit_type']
new_df['Unit Price'] = processing['unit_price']
new_df['Paid Amount'] = processing['amount']
new_df['Product Amount'] = mach_rent['amount']

# hub info
hub_comb_df = pd.DataFrame()
new_df['User ID'] = processing['user_id']
new_df['User'] = processing['user_name']
new_df['User Type'] = processing['user_type']
# region and franchisee
hub_comb_df['User ID'] = new_df['User ID']
hub_comb_df['Combined'] = new_df['User ID'].apply(hub_info_lookup)
hub_comb_df['Region']= hub_comb_df['Combined'].apply(region_lookup)
hub_comb_df['Franchisee']= hub_comb_df['Combined'].apply(franchisee_lookup)
new_df['Region'] = hub_comb_df['Region']
new_df['Franchisee'] = hub_comb_df['Franchisee']

# usd numbers
new_df['Currency Rate'] = 1/processing["currency_exchange_rate"]
new_df['Paid Amount USD'] = new_df['Paid Amount'].astype(float) * new_df['Currency Rate']

toc = time.time()
print(toc - tic, ' seconds')

processing_flagged = pd.DataFrame.copy(new_df)

new_df = pd.DataFrame(columns = cols)
# transaction features
tic = time.time()

new_df['Transaction ID'] = expenses['transaction_id']
new_df['Country'] = country
new_df['Transaction Type Level 2'] = 'Expense'
new_df['Date of Transaction'] = expenses['transaction_date']

new_df['Transaction Type'] = expenses['expense_category'].apply(trans_type_lookup)

# Product
new_df['Product'] = expenses['expense_type']
new_df['Product Category'] = expenses['expense_category']
# products_comb_df = pd.DataFrame()
# products_comb_df['Combined'] = new_df['Product'] + ":" + new_df['Product Category']
# products_comb_df['ID'] = products_comb_df['Combined'].apply(product_id_lookup_combined)
# new_df['Product ID'] = products_comb_df['ID']

print('Here')

# customer info
# new_df['Customer ID'] = sales['customer_id']
# new_df['Customer Name'] = sales['customer_name']
# new_df['Phone Number'] = sales['customer_mobile']
new_df['Market Type'] = "Farmers' Hub"

# basic info
# new_df['Quantity'] = expenses['quantity']
# new_df['Unit Type'] = expenses['unit_type']
# new_df['Unit Price'] = expenses['unit_price']
# new_df['Product Amount'] = expenses['total_amount']
new_df['Paid Amount'] = expenses['total_amount']
new_df['Product Amount'] = expenses['total_amount']

# hub info
hub_comb_df = pd.DataFrame()
new_df['User ID'] = expenses['user_id']
new_df['User'] = expenses['user_name']
new_df['User Type'] = expenses['user_type']
# region and franchisee
hub_comb_df['User ID'] = new_df['User ID']
hub_comb_df['Combined'] = new_df['User ID'].apply(hub_info_lookup)
hub_comb_df['Region']= hub_comb_df['Combined'].apply(region_lookup)
hub_comb_df['Franchisee']= hub_comb_df['Combined'].apply(franchisee_lookup)
new_df['Region'] = hub_comb_df['Region']
new_df['Franchisee'] = hub_comb_df['Franchisee']

# usd numbers
new_df['Currency Rate'] = 1/expenses["currency_exchange_rate"]
new_df['Paid Amount USD'] = new_df['Paid Amount'].astype(float) * new_df['Currency Rate']

toc = time.time()
print(toc - tic, ' seconds')
expenses_flagged = pd.DataFrame.copy(new_df)

# combine all
bd_data_all = pd.DataFrame(columns = cols)
bd_data_all = bd_data_all.append(other = sales_flagged, ignore_index=True)
bd_data_all = bd_data_all.append(other = purchasing_flagged, ignore_index=True)
bd_data_all = bd_data_all.append(other = mach_pur_flagged, ignore_index=True)
bd_data_all = bd_data_all.append(other = machine_rent_flagged, ignore_index=True)
bd_data_all = bd_data_all.append(other = processing_flagged, ignore_index=True)
bd_data_all = bd_data_all.append(other = expenses_flagged, ignore_index=True)
df = pd.DataFrame.copy(bd_data_all)
df_revenue = df[ ( df['Transaction Type Level 2'] == 'Sale') | ( df['Transaction Type Level 2'] == 'Machinery Rental') ]
df.at[df_revenue.index, 'Revenue'] = df_revenue['Paid Amount']
df['Net Profit'] = df['Revenue'] - df['COGS']


df = df.set_index('Transaction ID')

# write back to SQL database
df.to_sql('master_data', con=engine, if_exists='append')

# Close database connection
engine.close()