#Import required packages
import argparse
import json
import os
import pyspark.sql.functions as F

from pyspark.sql import SparkSession
from datetime import date, datetime, timedelta



#Create a Spark Session
spark = SparkSession.builder.getOrCreate()


def get_params():
  """ 
  Get the required arguments 
  """
  try:
    parser = argparse.ArgumentParser(description='Arguments')
    parser.add_argument('--customers_location', type=str, required=False, default="./input_data/starter/customers.csv", help="Path for Input Data of Customers")
    parser.add_argument('--products_location', type=str, required=False, default="./input_data/starter/products.csv", help="Path for Input Data of Products")
    parser.add_argument('--transactions_location', type=str, required=False, default="./input_data/starter/transactions/", help="Path for Input Data of Transactions")
    parser.add_argument('--output_location', type=str, required=False, default="./output_data/outputs/", help="Path for Output Data")
    #Keeping required as False for testing, can be passed at runtime to generate data accordingly
    parser.add_argument('--load_data_start_date', type=str, required=False, default="2018-12-01", help="Load Data Start Date")
    #Keeping required as False for testing, can be passed at runtime to generate data accordingly
    parser.add_argument('--load_data_end_date', type=str, required=False, default="2018-12-07", help="Load Data End Date")
    parser.add_argument('--debug', type=bool, required=False, default=False, help="Debug mode, pass True for debugging")
    return vars(parser.parse_args())
  except Exception as e:
    print('Error in get_params() function :')
    print(e)

    
    
def read_input_data(params):
  """ 
  Read the input data for Customer, Products and Transactions from the location
  """
  try:
    if params['debug']:
      print('Inside read_input_data Function')
    #Read Customers Data 
    cust_data_df = spark.read.format('csv').option("header","True").option("inferSchema","True").load(params['customers_location'])
    if params['debug']:
      print('Customers Data -')
      cust_data_df.show(10)
      print('Customers Data Count -')
      print(cust_data_df.count())
    #Create Temporary View
    cust_data_df.createOrReplaceTempView('customers_data')
    #Read Products Data 
    prod_data_df = spark.read.format('csv').option("header","True").option("inferSchema","True").load(params['products_location'])
    if params['debug']:
      print('Products Data -')
      prod_data_df.show(10)
      print('Products Data Count -')
      print(prod_data_df.count())
    #Create Temporary View
    prod_data_df.createOrReplaceTempView('products_data')
    #Read Transactions Data
    load_start_date = params['load_data_start_date']
    load_end_date = params['load_data_end_date']
    start_date = datetime.strptime(load_start_date,"%Y-%m-%d").date()
    end_date = datetime.strptime(load_end_date,"%Y-%m-%d").date()
    #List of Dates
    date_list = []
    delta = timedelta(days=1)
    input_date = start_date
    #Get all the dates to be read from input location for transactions data
    while input_date <= end_date:
      date_list.append(input_date)
      input_date = input_date + delta
    #Convert date list to str list
    date_list_str = [date_obj.strftime('%Y-%m-%d') for date_obj in date_list]
    count=0
    for trans_date in date_list_str:
      path = "{file_loc}d={date}/transactions.json".format(file_loc=params['transactions_location'],date=trans_date)
      df = spark.read.json(path)
      if count==0:
        final_df = df
      if count>0:
        final_df = final_df.union(df)
      count = count + 1  
    trans_df = final_df
    #Explode the columns - from array to columns
    df=trans_df.withColumn('exploded', F.explode('basket'))
    #Get the price and product_id in the new columns
    df=df.withColumn('price', F.col('exploded').getItem('price'))
    df=df.withColumn('product_id', F.col('exploded').getItem('product_id'))
    trans_data_df = df.select('customer_id','date_of_purchase','price','product_id')
    if params['debug']:
      print('Transactions Data -')
      trans_data_df.show(10)
      print('Transactions Data Count -')
      print(trans_data_df.count())
    #Create Temporary View
    trans_data_df.createOrReplaceTempView('transactions_data')
    print('Done Reading Data of Customers, Products and Transactions')
  except Exception as e:
    print('Error in read_input_data() function :')
    print(e)

      
      
      
def transform_func(params):
  """ 
  Transformation function for getting required customer details
  """
  try:
    if params['debug']:
      print('Inside transform_func Function')
    cust_details_df = spark.sql(""" select cust.customer_id, cust.loyalty_score, 
                           prod.product_id, prod.product_category,
                           count(1) as purchase_count
                           from transactions_data trans
                           inner join 
                           customers_data cust 
                           on trans.customer_id = cust.customer_id
                           inner join
                           products_data prod
                           on prod.product_id = trans.product_id
                           group by cust.customer_id, cust.loyalty_score, 
                           prod.product_id, prod.product_category
                        """)
    if params['debug']:
      print('Customer Details Data -')
      cust_details_df.show(10)
      print('Customer Details Data Count -')
      print(cust_details_df.count())
    print('Done Getting Customers Details')
    return cust_details_df
  except Exception as e:
    print('Error in transform_func() function :')
    print(e)
  
  
  
def load_data(params, cust_details_df):
  """ 
  Reads Spark DataFrame and Loads Data into JSON Format at the output location path
  """
  try:
    if params['debug']:
      print('Inside load_data Function')
    #Today's Date
    todays_date = date.today()
    todays_date_str = todays_date.strftime('%Y-%m-%d')
    #Create Output Directory if not exists
    os.makedirs(params['output_location'], exist_ok=True)
    #Output Path - It will create a folder based on current/ todays date and data gets loaded into customer_details.json file
    output_path = "{file_loc}d={date}/customer_details.json".format(file_loc=params['output_location'],date=todays_date_str)
    #Write output in the output_location path in the JSON Format
    print('Writing Output to JSON File at path - '+ output_path)
    cust_details_df.coalesce(1).write.mode('overwrite').format('json').save(output_path)
    print('Data has been successfully loaded!')
  except Exception as e:
    print('Error in load_data() function :')
    print(e)

    
    
def main():
  """
  Main Function - Involves the ETL Functions
  Performs Extract, Transform and Loads Data 
  """
  try:
    #Get Parameters to be passed
    params = get_params()
    if params['debug']:
      print('Parameters passed :')
      print(params)
    #Extract/ Read Input Data of Customers, Products and Transactions
    read_input_data(params)
    #Transform Data - Get the required data
    cust_details_df = transform_func(params)
    #Load Data into JSON format
    load_data(params, cust_details_df)
  except Exception as e:
    print('Error in main() function :')
    print(e)

    
    
if __name__ == "__main__":
  try:
    main()
  except Exception as e:
    print('Error :')
    print(e)
  finally:
    print('Execution Complete')
    spark.stop()
