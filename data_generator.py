import csv

header = ['order_id', 'customer_id', 'customer_name', 'product_id', 'product_name', 
'product_category', 'payment_type', 'qty', 'price', 'datetime', 'country', 'city', 
'ecommerce_website_name', 'payment_txn_id', 'payment_txn_success', 'failure_reason']

data = [

]

# open the file in write mode
with open('created_data.csv', 'w', newline='') as f:

    # create the csv writer
    writer = csv.writer(f)
    # write the header
    writer.writerow(header)
    # write the data
    writer.writerows(data)

    product_name ()

    product_category ('video game', 'board game', 'card game', )

    country ()

    city ()

    ecommerce_website_name ()

    failure_reason ()

