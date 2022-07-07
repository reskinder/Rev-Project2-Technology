import csv
import random
import names as nm
from faker import Faker

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



# function to create data
def make_data(num):

    fake = Faker('en_US')

    # Pools to be randomly selected from
    product_name_video_game = [[1, 'Breath of the Wild', 60], [2, 'Super Mario Odyssey', 60], 
    [3, 'Pokémon Soulsilver', 40], [4, 'GTAV', 60], [5, 'Red Dead Redemption II', 60], 
    [6, 'Undertale', 25], [7, 'Cyberpunk 2077', 60], [8, 'It Takes Two', 40], [9, 'Overwatch', 60], 
    [10, 'Fall Guys', 20], [11, 'The Sims 4', 40], [12, 'Overcooked 2', 40], [13, 'Borderlands 2', 40]]

    product_name_board_game = [[14, 'Monopoly', 20], [15, 'Sorry', 20], [16, 'Trouble', 20], 
    [17, 'Settlers of Catan', 20], [18, 'Chutes and Ladders', 20],[19, 'Candy Land', 20], 
    [20, 'The Game of Life', 20], [21, 'Clue', 20], [22, 'Battleship', 20], [23, 'Mouse Trap', 20],
     [24, 'Cranium', 20], [25, 'Scrabble', 20]]

    product_name_card_game = [[26, 'Standard Deck of Cards', 10], [27, 'Cards Against Humanity', 25],
    [28, 'Exploding Kittens', 20], [29, 'Coup', 15], [30, 'Uno', 5], [31, 'Magic:The Gathering', 50], 
    [32, 'Yu-Gi-Oh:TCG', 30], [33, 'Pokémon:TCG', 30], [34, 'Apples to Apples', 25], [35, 'Monopoly:Deal', 10]]

    product_name_doll = [[36, 'Barbie', 20], [37, 'Jade', 20], [38, 'Sasha', 20], [39, 'Yasmin', 20], 
    [40, 'Chloe', 20], [41, 'Superman', 30], [42, 'Leonardo', 35], [43, 'Donatello', 35], [44, 'Raphael', 35], 
    [45, 'Michaelangelo', 35], [46, 'The Hulk', 30], [47, 'Captain America', 25], [48, 'Iron Man', 25]]

    product_category = ['Video Game', 'Board Game', 'Card Game', 'Doll']

    payment_type = ['Card', 'Internet Banking', 'UPI', 'Wallet']
    
    
    fake_data = [{'order_id': x+1,
                'customer_id': 101+x,
                'customer_name': fake.name(),
                'product_id': '',
                'product_name': '',
                'product_category': random.choice(product_category),
                'payment_type': random.choice(payment_type),
                'qty': random.randint(1,51),
                'price': 'TBD',
                'datetime': (f'{fake.date_time_this_decade()}'),
                'country': 'TBD',
                'city': 'TBD',
                'ecommerce_website_name': 'TBD',
                'payment_txn_id':50+x,
                'payment_txn_success': 'TBD',
                'failure_reason': 'TBD',
                } for x in range(num)
                ]
    for i in fake_data:
        if i['product_category'] == 'Video Game':
            i['product_name'] = random.choice(product_name_video_game)[1]
        elif i['product_category'] == 'Board Game':
            i['product_name'] = random.choice(product_name_board_game)[1] 
        elif i['product_category'] == 'Card Game':
            i['product_name'] = random.choice(product_name_card_game)[1]
        elif i['product_category'] == 'Doll':
            i['product_name'] = random.choice(product_name_doll)[1]
    # for i in fake_data:
        
    
    print(fake_data)

make_data(5)



# country ()

# city ()

# ecommerce_website_name ()

# failure_reason ()

