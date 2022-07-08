import csv
import pandas as pd
import random
from faker import Faker

# Output csv file headers (fields)
header = ['order_id', 'customer_id', 'customer_name', 'product_id', 'product_name', 
'product_category', 'payment_type', 'qty', 'price', 'datetime', 'country', 'city', 
'ecommerce_website_name', 'payment_txn_id', 'payment_txn_success', 'failure_reason']
 
# Output csv file data (description)
data = []

# Function to create data
def make_data(num):
    # Faker object
    fake = Faker('en_US')

    # Pools to be randomly selected from for product name, category, & payment type
    product_name_video_game = [[1, 'Breath of the Wild', 60], [2, 'Super Mario Odyssey', 60], 
    [3, 'Pokémon Soulsilver', 40], [4, 'GTAV', 60], [5, 'Red Dead Redemption II', 60], 
    [6, 'Undertale', 25], [7, 'Cyberpunk 2077', 60], [8, 'It Takes Two', 40], [9, 'Overwatch', 60], 
    [10, 'Fall Guys', 20], [11, 'The Sims 4', 40], [12, 'Overcooked 2', 40], [13, 'Borderlands 2', 40]]

    product_name_board_game = [[14, 'Monopoly', 20], [15, 'Sorry', 20], [16, 'Trouble', 20], 
    [17, 'Settlers of Catan', 20], [18, 'Chutes and Ladders', 20],[19, 'Candy Land', 20], 
    [20, 'The Game of Life', 20], [21, 'Clue', 20], [22, 'Battleship', 20], [23, 'Mouse Trap', 20],
    [24, 'Cranium', 20], [25, 'Scrabble', 20]]

    product_name_card_game = [[26, 'Standard Deck of Cards', 10], [27, 'Cards Against Humanity', 25],
    [28, 'Exploding Kittens', 20], [29, 'Coup', 15], [30, 'Uno', 5], [31, 'Magic: The Gathering', 50], 
    [32, 'Yu-Gi-Oh: TCG', 30], [33, 'Pokémon: TCG', 30], [34, 'Apples to Apples', 25], [35, 'Monopoly:Deal', 10]]

    product_name_doll = [[36, 'Barbie', 20], [37, 'Jade', 20], [38, 'Sasha', 20], [39, 'Yasmin', 20], 
    [40, 'Chloe', 20], [41, 'Superman', 30], [42, 'Leonardo', 35], [43, 'Donatello', 35], [44, 'Raphael', 35], 
    [45, 'Michaelangelo', 35], [46, 'The Hulk', 30], [47, 'Captain America', 25], [48, 'Iron Man', 25]]

    product_category = ['Video Game', 'Board Game', 'Card Game', 'Doll']

    payment_type = ['Card', 'Internet Banking', 'UPI', 'Wallet']
    
    # Fake data list
    fake_data = [{'order_id': x+1,
                'customer_id': 101+x,
                'customer_name': fake.name(),
                'product_id': '',
                'product_name': '',
                'product_category': random.choice(product_category),
                'payment_type': random.choice(payment_type),
                'qty': random.randint(1,51),
                'price': '',
                'datetime': (f'{fake.date_time_this_decade()}'),
                'country': 'TBD',
                'city': 'TBD',
                'ecommerce_website_name': 'TBD',
                'payment_txn_id': 'TBD',
                'payment_txn_success': 'TBD',
                'failure_reason': 'TBD',
                } for x in range(num)
                ]
    
    # Product name, id, price
    for i in fake_data:
        if i['product_category'] == 'Video Game':
            rand_index = random.randint(0,12)
            i['product_name'] = product_name_video_game[rand_index][1]
            i['product_id'] = product_name_video_game[rand_index][0]
            i['price'] = (product_name_video_game[rand_index][2])*i['qty']
        elif i['product_category'] == 'Board Game':
            rand_index = random.randint(0,11)
            i['product_name'] = product_name_board_game[rand_index][1]
            i['product_id'] = product_name_board_game[rand_index][0]
            i['price'] = (product_name_board_game[rand_index][2])*i['qty']
        elif i['product_category'] == 'Card Game':
            rand_index = random.randint(0,9)
            i['product_name'] = product_name_card_game[rand_index][1]
            i['product_id'] = product_name_card_game[rand_index][0]
            i['price'] = (product_name_card_game[rand_index][2])*i['qty']
        elif i['product_category'] == 'Doll':
            rand_index = random.randint(0,12)
            i['product_name'] = product_name_doll[rand_index][1]
            i['product_id'] = product_name_doll[rand_index][0]
            i['price'] = (product_name_doll[rand_index][2])*i['qty']
    
    # Location (city, country)
    locationList = [['New York City', 'USA'], ['Chicago', 'USA'], ['Charleston', 'USA'], ['Las Vegas', 'USA'], ['Seattle', 'USA'], 
    ['San Francisco', 'USA'], ['Washington, DC', 'USA'], ['New Orleans', 'USA'], ['Palm Springs', 'USA'], ['San Diego', 'USA'], 
    ['St. Louis', 'USA'], ['Sedona', 'USA'], ['Honolulu', 'USA'], ['Miami Beach', 'USA'], ['Branson', 'USA'], ['Boston', 'USA'], 
    ['Savannah', 'USA'], ['Orlando', 'USA'], ['Portland', 'USA'], ['Laihaina', 'USA'], ['St. Augustine', 'USA'], ['Nashville', 'USA'], 
    ['Los Angeles', 'USA'], ['San Antonio', 'USA'], ['Austin', 'USA'], ['Berlin', 'Germany'], ['Munich', 'Germany'], 
    ['Hamburg', 'Germany'], ['Cologne', 'Germany'], ['Frankfurt', 'Germany'], ['Leipzig', 'Germany'], ['Düsseldorf', 'Germany'], 
    ['Stuttgart', 'Germany'], ['Dresden', 'Germany'], ['Nuremburg', 'Germany'], ['Hanover', 'Germany'], ['Mexico City', 'Mexico'], 
    ['Puerto Vallarta', 'Mexico'], ['Guadalajara', 'Mexico'], ['Oaxaca', 'Mexico'], ['Playa del Carmen', 'Mexico'], 
    ['San Miguel de Allende', 'Mexico'], ['Tijuana', 'Mexico'], ['Tulum', 'Mexico'], ['Acapulco', 'Mexico'], ['Monterrey', 'Mexico'], 
    ['Lagos', 'Nigeria'], ['Ibadan', 'Nigeria'], ['Benin City', 'Nigeria'], ['Enugu', 'Nigeria'], ['Port Harcourt', 'Nigeria'], 
    ['Onitsha', 'Nigeria'], ['Zaria', 'Nigeria'], ['Kano', 'Nigeria'], ['Abuja', 'Nigeria'], ['Aba', 'Nigeria'], ['Owerri', 'Nigeria'], 
    ['Shanghai', 'China'], ['Chengdu', 'China'], ['Chongqing', 'China'], ['Beijing', 'China'], ['Shenzhen', 'China'], 
    ['Suzhou', 'China'], ['Guangzhou', 'China'], ['Hangzhou', 'China'], ['Wuhan', 'China'], ['Harbin', 'China'], ['Mumbai', 'India'], 
    ['Hyderabad', 'India'], ['Pune', 'India'], ['Bengaluru', 'India'], ['Kolkata', 'India'], ['Surat', 'India'], ['Jaipur', 'India'], 
    ['Chennai', 'India'], ['Ahmedabad', 'India'], ['New Delhi', 'India'], ['Quebec City', 'Canada'], ['Toronto', 'Canada'], 
    ['Winnipeg', 'Canada'], ['Vancouver', 'Canada'], ['Ottawa', 'Canada'], ['Edmonton', 'Canada'], ['Montreal', 'Canada'], 
    ['Calgary', 'Canada'], ['Victoria', 'Canada'], ['Regina', 'Canada'], ['Sydney', 'Australia'], ['Melbourne', 'Australia'], 
    ['Adelaide', 'Australia'], ['Hobart', 'Australia'], ['Gold Coast', 'Australia'], ['Perth', 'Australia'], 
    ['Brisbane', 'Australia'], ['Canberra', 'Australia'], ['Cairns', 'Australia'], ['Darwin', 'Australia'], 
    ['London', 'UK'], ['Liverpool', 'UK'], ['Cardiff', 'UK'], ['Bristol', 'UK'], ['Manchester', 'UK'], ['Birmingham', 'UK'], 
    ['Edinburgh', 'UK'], ['City of London', 'UK'], ['Glagow', 'UK'], ['Belfast', 'UK'], ['Rio de Janeiro', 'Brazil'], 
    ['Manaus', 'Brazil'], ['Belo Horizonte', 'Brazil'], ['Salvador', 'Brazil'], ['Fortaleza', 'Brazil'], 
    ['Brasilia', 'Brazil'], ['Sao Paulo', 'Brazil'], ['Curitiba', 'Brazil'], ['Recife', 'Brazil'], ['Natal', 'Brazil'], 
    ['Tokyo', 'Japan'], ['Sapporo', 'Japan'], ['Hiroshima', 'Japan'], ['Kyoto', 'Japan'], ['Yokohama', 'Japan'], ['Kobe', 'Japan'], 
    ['Osaka', 'Japan'], ['Fukuoka', 'Japan'], ['Nara', 'Japan'], ['Nagoya', 'Japan'], ['Moscow', 'Russia'], ['Kazan', 'Russia'], 
    ['Saint Petersburg', 'Russia'], ['Ufa', 'Russia'], ['Nizhny Novgorod', 'Russia'], ['Yekaterinburg', 'Russia'], 
    ['Novosibirsk', 'Russia'], ['Chelyabinsk', 'Russia'], ['Sochi', 'Russia'], ['Samara', 'Russia'], ['Bamako', 'Mali'], 
    ['Timbuktu', 'Mali'], ['Douala', 'Cameroon'], ['Yaounde', 'Cameroon'], ['Garoua', 'Cameroon'], ['Cape Town', 'South Africa'], 
    ['Gqeberha', 'South Africa'], ['East London', 'South Africa'], ['Johannesburg', 'South Africa'], ['Pretoria', 'South Africa'], 
    ['Pietermaritzburg', 'South Africa'], ['Durban', 'South Africa'], ['Bloemfontein', 'South Africa'], 
    ['Kimberley', 'South Africa'], ['Soweto', 'South Africa']]

    for i in fake_data:
        location = random.randint(0,len(locationList)-1)
        i['city'] = locationList[location][0]
        i['country'] = locationList[location][1]
    
    # Ecommerce Website
    df = pd.read_csv("ecommerce_websites.csv")

    countriesDict = {
        "USA": df['USA'].tolist(),
        "Germany": df['Germany'].tolist(),
        "Brazil": df['Brazil'].tolist(),
        "Austraila": df['Australia'].tolist(),
        "Japan": df['Japan'].tolist(),
        "China": df['China'].tolist(),
        "India": df['India'].tolist(),
        "Mexico": df['Mexico'].tolist(),
        "Canada": df['Canada'].tolist(),
        "UK": df['UK'].tolist(),
        "Russia": df['Russia'].tolist(),
        "Mali": df['Mali'].tolist(),
        "Cameroon": df['Cameroon'].tolist(),
        "South Africa": df['South Africa'].tolist(),
        "Nigeria": df['Nigeria'].tolist(),
    }

    for i in fake_data:
        countryPick = i['country']

        if countryPick == "USA":
            sitePick = random.choice(countriesDict[countryPick])
        elif countryPick == "Germany":
            sitePick = random.choice(countriesDict[countryPick])
        elif countryPick == "Brazil":
            sitePick = random.choice(countriesDict[countryPick])
        elif countryPick == "Austraila":
            sitePick = random.choice(countriesDict[countryPick])
        elif countryPick == "Japan":
            sitePick = random.choice(countriesDict[countryPick])
        elif countryPick == "China":
            sitePick = random.choice(countriesDict[countryPick])
        elif countryPick == "India":
            sitePick = random.choice(countriesDict[countryPick])
        elif countryPick == "Mexico":
            sitePick = random.choice(countriesDict[countryPick])
        elif countryPick == "Canada":
            sitePick = random.choice(countriesDict[countryPick])
        elif countryPick == "UK":
            sitePick = random.choice(countriesDict[countryPick])
        elif countryPick == "Russia":
            sitePick = random.choice(countriesDict[countryPick])
        elif countryPick == "Mali":
            sitePick = random.choice(countriesDict[countryPick])
        elif countryPick == "Cameroon":
            sitePick = random.choice(countriesDict[countryPick])
        elif countryPick == "South Africa":
            sitePick = random.choice(countriesDict[countryPick])
        elif countryPick == "Nigeria":
            sitePick = random.choice(countriesDict[countryPick])
        
        i['ecommerce_website_name'] = sitePick
    
    # Payment Txn ID
    transactionID = list(range(10000,99999))

    for i in fake_data:
        randTransaction = random.sample(transactionID, k=1)
        for j in randTransaction:
            i['payment_txn_id'] = j
    
    # Payment Success/Failure & Failure Reason
    failList = ["authentication failure", "invalid method", "insufficient funds", "processor declined", 
    "account suspended", "expired card", "invalid address", "invalid cvv"]
    
    for i in fake_data:
        validity = ["Y","N"]
        payTest = random.choices(validity, weights=(95,5), k=1)
        if "N" in payTest:
            i['payment_txn_success'] = 'N'
            i['failure_reason'] = random.choice(failList)
        if "Y" in payTest:
            i['payment_txn_success'] = 'Y'
            i['failure_reason'] = ''
    
    # Return fake_data
    return fake_data

# Function to create rogue data
def make_rogue_data(num):
    # Faker object
    fake = Faker('en_US')

    # Pools to be randomly selected from for product name, category, & payment type
    product_name_video_game = [[1, 'Breath of the Wild', 60], [2, 'Super Mario Odyssey', 60], 
    [3, 'Pokémon Soulsilver', 40], [4, 'GTAV', 60], [5, 'Red Dead Redemption II', 60], 
    [6, 'Undertale', 25], [7, 'Cyberpunk 2077', 60], [8, 'It Takes Two', 40], [9, 'Overwatch', 60], 
    [10, 'Fall Guys', 20], [11, 'The Sims 4', 40], [12, 'Overcooked 2', 40], [13, 'Borderlands 2', 40]]

    product_name_board_game = [[14, 'Monopoly', 20], [15, 'Sorry', 20], [16, 'Trouble', 20], 
    [17, 'Settlers of Catan', 20], [18, 'Chutes and Ladders', 20],[19, 'Candy Land', 20], 
    [20, 'The Game of Life', 20], [21, 'Clue', 20], [22, 'Battleship', 20], [23, 'Mouse Trap', 20],
    [24, 'Cranium', 20], [25, 'Scrabble', 20]]

    product_name_card_game = [[26, 'Standard Deck of Cards', 10], [27, 'Cards Against Humanity', 25],
    [28, 'Exploding Kittens', 20], [29, 'Coup', 15], [30, 'Uno', 5], [31, 'Magic: The Gathering', 50], 
    [32, 'Yu-Gi-Oh: TCG', 30], [33, 'Pokémon: TCG', 30], [34, 'Apples to Apples', 25], [35, 'Monopoly:Deal', 10]]

    product_name_doll = [[36, 'Barbie', 20], [37, 'Jade', 20], [38, 'Sasha', 20], [39, 'Yasmin', 20], 
    [40, 'Chloe', 20], [41, 'Superman', 30], [42, 'Leonardo', 35], [43, 'Donatello', 35], [44, 'Raphael', 35], 
    [45, 'Michaelangelo', 35], [46, 'The Hulk', 30], [47, 'Captain America', 25], [48, 'Iron Man', 25]]

    product_category = ['Video Game', 'Board Game', 'Card Game', 'Doll']

    payment_type = ['Card', 'Internet Banking', 'UPI', 'Wallet']
    
    # Fake data list
    fake_data = [{'order_id': x+9501,
                'customer_id': 9601+x,
                'customer_name': fake.name(),
                'product_id': '',
                'product_name': '',
                'product_category': random.choice(product_category),
                'payment_type': random.choice(payment_type),
                'qty': random.randint(1,51),
                'price': '',
                'datetime': (f'{fake.date_time_this_decade()}'),
                'country': 'TBD',
                'city': 'TBD',
                'ecommerce_website_name': 'TBD',
                'payment_txn_id': 'TBD',
                'payment_txn_success': '',
                'failure_reason': '',
                } for x in range(num)
                ]
    
    # Product name, id, price
    for i in fake_data:
        if i['product_category'] == 'Video Game':
            rand_index = random.randint(0,12)
            i['product_name'] = product_name_video_game[rand_index][1]
            i['product_id'] = product_name_video_game[rand_index][0]
            i['price'] = (product_name_video_game[rand_index][2])*i['qty']
        elif i['product_category'] == 'Board Game':
            rand_index = random.randint(0,11)
            i['product_name'] = product_name_board_game[rand_index][1]
            i['product_id'] = product_name_board_game[rand_index][0]
            i['price'] = (product_name_board_game[rand_index][2])*i['qty']
        elif i['product_category'] == 'Card Game':
            rand_index = random.randint(0,9)
            i['product_name'] = product_name_card_game[rand_index][1]
            i['product_id'] = product_name_card_game[rand_index][0]
            i['price'] = (product_name_card_game[rand_index][2])*i['qty']
        elif i['product_category'] == 'Doll':
            rand_index = random.randint(0,12)
            i['product_name'] = product_name_doll[rand_index][1]
            i['product_id'] = product_name_doll[rand_index][0]
            i['price'] = (product_name_doll[rand_index][2])*i['qty']
    
    # Location (city, country)
    locationList = [['New York City', 'USA'], ['Chicago', 'USA'], ['Charleston', 'USA'], ['Las Vegas', 'USA'], ['Seattle', 'USA'], 
    ['San Francisco', 'USA'], ['Washington, DC', 'USA'], ['New Orleans', 'USA'], ['Palm Springs', 'USA'], ['San Diego', 'USA'], 
    ['St. Louis', 'USA'], ['Sedona', 'USA'], ['Honolulu', 'USA'], ['Miami Beach', 'USA'], ['Branson', 'USA'], ['Boston', 'USA'], 
    ['Savannah', 'USA'], ['Orlando', 'USA'], ['Portland', 'USA'], ['Laihaina', 'USA'], ['St. Augustine', 'USA'], ['Nashville', 'USA'], 
    ['Los Angeles', 'USA'], ['San Antonio', 'USA'], ['Austin', 'USA'], ['Berlin', 'Germany'], ['Munich', 'Germany'], 
    ['Hamburg', 'Germany'], ['Cologne', 'Germany'], ['Frankfurt', 'Germany'], ['Leipzig', 'Germany'], ['Düsseldorf', 'Germany'], 
    ['Stuttgart', 'Germany'], ['Dresden', 'Germany'], ['Nuremburg', 'Germany'], ['Hanover', 'Germany'], ['Mexico City', 'Mexico'], 
    ['Puerto Vallarta', 'Mexico'], ['Guadalajara', 'Mexico'], ['Oaxaca', 'Mexico'], ['Playa del Carmen', 'Mexico'], 
    ['San Miguel de Allende', 'Mexico'], ['Tijuana', 'Mexico'], ['Tulum', 'Mexico'], ['Acapulco', 'Mexico'], ['Monterrey', 'Mexico'], 
    ['Lagos', 'Nigeria'], ['Ibadan', 'Nigeria'], ['Benin City', 'Nigeria'], ['Enugu', 'Nigeria'], ['Port Harcourt', 'Nigeria'], 
    ['Onitsha', 'Nigeria'], ['Zaria', 'Nigeria'], ['Kano', 'Nigeria'], ['Abuja', 'Nigeria'], ['Aba', 'Nigeria'], ['Owerri', 'Nigeria'], 
    ['Shanghai', 'China'], ['Chengdu', 'China'], ['Chongqing', 'China'], ['Beijing', 'China'], ['Shenzhen', 'China'], 
    ['Suzhou', 'China'], ['Guangzhou', 'China'], ['Hangzhou', 'China'], ['Wuhan', 'China'], ['Harbin', 'China'], ['Mumbai', 'India'], 
    ['Hyderabad', 'India'], ['Pune', 'India'], ['Bengaluru', 'India'], ['Kolkata', 'India'], ['Surat', 'India'], ['Jaipur', 'India'], 
    ['Chennai', 'India'], ['Ahmedabad', 'India'], ['New Delhi', 'India'], ['Quebec City', 'Canada'], ['Toronto', 'Canada'], 
    ['Winnipeg', 'Canada'], ['Vancouver', 'Canada'], ['Ottawa', 'Canada'], ['Edmonton', 'Canada'], ['Montreal', 'Canada'], 
    ['Calgary', 'Canada'], ['Victoria', 'Canada'], ['Regina', 'Canada'], ['Sydney', 'Australia'], ['Melbourne', 'Australia'], 
    ['Adelaide', 'Australia'], ['Hobart', 'Australia'], ['Gold Coast', 'Australia'], ['Perth', 'Australia'], 
    ['Brisbane', 'Australia'], ['Canberra', 'Australia'], ['Cairns', 'Australia'], ['Darwin', 'Australia'], 
    ['London', 'UK'], ['Liverpool', 'UK'], ['Cardiff', 'UK'], ['Bristol', 'UK'], ['Manchester', 'UK'], ['Birmingham', 'UK'], 
    ['Edinburgh', 'UK'], ['City of London', 'UK'], ['Glagow', 'UK'], ['Belfast', 'UK'], ['Rio de Janeiro', 'Brazil'], 
    ['Manaus', 'Brazil'], ['Belo Horizonte', 'Brazil'], ['Salvador', 'Brazil'], ['Fortaleza', 'Brazil'], 
    ['Brasilia', 'Brazil'], ['Sao Paulo', 'Brazil'], ['Curitiba', 'Brazil'], ['Recife', 'Brazil'], ['Natal', 'Brazil'], 
    ['Tokyo', 'Japan'], ['Sapporo', 'Japan'], ['Hiroshima', 'Japan'], ['Kyoto', 'Japan'], ['Yokohama', 'Japan'], ['Kobe', 'Japan'], 
    ['Osaka', 'Japan'], ['Fukuoka', 'Japan'], ['Nara', 'Japan'], ['Nagoya', 'Japan'], ['Moscow', 'Russia'], ['Kazan', 'Russia'], 
    ['Saint Petersburg', 'Russia'], ['Ufa', 'Russia'], ['Nizhny Novgorod', 'Russia'], ['Yekaterinburg', 'Russia'], 
    ['Novosibirsk', 'Russia'], ['Chelyabinsk', 'Russia'], ['Sochi', 'Russia'], ['Samara', 'Russia'], ['Bamako', 'Mali'], 
    ['Timbuktu', 'Mali'], ['Douala', 'Cameroon'], ['Yaounde', 'Cameroon'], ['Garoua', 'Cameroon'], ['Cape Town', 'South Africa'], 
    ['Gqeberha', 'South Africa'], ['East London', 'South Africa'], ['Johannesburg', 'South Africa'], ['Pretoria', 'South Africa'], 
    ['Pietermaritzburg', 'South Africa'], ['Durban', 'South Africa'], ['Bloemfontein', 'South Africa'], 
    ['Kimberley', 'South Africa'], ['Soweto', 'South Africa']]

    for i in fake_data:
        location = random.randint(0,len(locationList)-1)
        i['city'] = locationList[location][0]
        i['country'] = locationList[location][1]
    
    # Ecommerce Website
    df = pd.read_csv("ecommerce_websites.csv")

    countriesDict = {
        "USA": df['USA'].tolist(),
        "Germany": df['Germany'].tolist(),
        "Brazil": df['Brazil'].tolist(),
        "Austraila": df['Australia'].tolist(),
        "Japan": df['Japan'].tolist(),
        "China": df['China'].tolist(),
        "India": df['India'].tolist(),
        "Mexico": df['Mexico'].tolist(),
        "Canada": df['Canada'].tolist(),
        "UK": df['UK'].tolist(),
        "Russia": df['Russia'].tolist(),
        "Mali": df['Mali'].tolist(),
        "Cameroon": df['Cameroon'].tolist(),
        "South Africa": df['South Africa'].tolist(),
        "Nigeria": df['Nigeria'].tolist(),
    }

    for i in fake_data:
        countryPick = i['country']

        if countryPick == "USA":
            sitePick = random.choice(countriesDict[countryPick])
        elif countryPick == "Germany":
            sitePick = random.choice(countriesDict[countryPick])
        elif countryPick == "Brazil":
            sitePick = random.choice(countriesDict[countryPick])
        elif countryPick == "Austraila":
            sitePick = random.choice(countriesDict[countryPick])
        elif countryPick == "Japan":
            sitePick = random.choice(countriesDict[countryPick])
        elif countryPick == "China":
            sitePick = random.choice(countriesDict[countryPick])
        elif countryPick == "India":
            sitePick = random.choice(countriesDict[countryPick])
        elif countryPick == "Mexico":
            sitePick = random.choice(countriesDict[countryPick])
        elif countryPick == "Canada":
            sitePick = random.choice(countriesDict[countryPick])
        elif countryPick == "UK":
            sitePick = random.choice(countriesDict[countryPick])
        elif countryPick == "Russia":
            sitePick = random.choice(countriesDict[countryPick])
        elif countryPick == "Mali":
            sitePick = random.choice(countriesDict[countryPick])
        elif countryPick == "Cameroon":
            sitePick = random.choice(countriesDict[countryPick])
        elif countryPick == "South Africa":
            sitePick = random.choice(countriesDict[countryPick])
        elif countryPick == "Nigeria":
            sitePick = random.choice(countriesDict[countryPick])
        
        i['ecommerce_website_name'] = sitePick
    
    # Payment Txn ID
    transactionID = list(range(10000,99999))

    for i in fake_data:
        randTransaction = random.sample(transactionID, k=1)
        for j in randTransaction:
            i['payment_txn_id'] = j
    
    # Return fake_data
    return fake_data

# open the file in write mode
with open('created_data.csv', 'w', newline='') as f:
    # create the csv writer
    writer = csv.DictWriter(f, fieldnames = header)
    # write the header
    writer.writeheader()
    # write the data
    writer.writerows(make_data(9500))
    writer.writerows(make_rogue_data(500))