import csv
from faker import Faker
import datetime
# from random import randint
# randint(1800, 1900)
import random


#list = ['card', 'Internet Banking', 'UPI', 'Wallet']
lst = [['New York City', 'USA'], ['Chicago', 'USA'], ['Charleston', 'USA'], ['Las Vegas', 'USA'], ['Seattle', 'USA'], ['San Francisco', 'USA'], ['Washington, DC', 'USA'], ['New Orleans', 'USA'], ['Palm Springs', 'USA'], ['San Diego', 'USA'], ['St. Louis', 'USA'], ['Sedona', 'USA'], ['Honolulu', 'USA'], ['Miami Beach', 'USA'], ['Branson', 'USA'], ['Boston', 'USA'], ['Savannah', 'USA'], ['Orlando', 'USA'], ['Portland', 'USA'], ['Laihaina', 'USA'], ['St. Augustine', 'USA'], ['Nashville', 'USA'], ['Los Angeles', 'USA'], ['San Antonio', 'USA'], ['Austin', 'USA'], ['Berlin', 'Germany'], ['Munich', 'Germany'], ['Hamburg', 'Germany'], ['Cologne', 'Germany'], ['Frankfurt', 'Germany'], ['Leipzig', 'Germany'], ['Düsseldorf', 'Germany'], ['Stuttgart', 'Germany'], ['Dresden', 'Germany'], ['Nuremburg', 'Germany'], ['Hanover', 'Germany'], ['Mexico City', 'Mexico'], ['Puerto Vallarta', 'Mexico'], ['Guadalajara', 'Mexico'], ['Oaxaca', 'Mexico'], ['Playa del Carmen', 'Mexico'], ['San Miguel de Allende', 'Mexico'], ['Tijuana', 'Mexico'], ['Tulum', 'Mexico'], ['Acapulco', 'Mexico'], ['Monterrey', 'Mexico'], ['Lagos', 'Nigeria'], ['Ibadan', 'Nigeria'], ['Benin City', 'Nigeria'], ['Enugu', 'Nigeria'], ['Port Harcourt', 'Nigeria'], ['Onitsha', 'Nigeria'], ['Zaria', 'Nigeria'], ['Kano', 'Nigeria'], ['Abuja', 'Nigeria'], ['Aba', 'Nigeria'], ['Owerri', 'Nigeria'], ['Shanghai', 'China'], ['Chengdu', 'China'], ['Chongqing', 'China'], ['Beijing', 'China'], ['Shenzhen', 'China'], ['Suzhou', 'China'], ['Guangzhou', 'China'], ['Hangzhou', 'China'], ['Wuhan', 'China'], ['Harbin', 'China'], ['Mumbai', 'India'], ['Hyderabad', 'India'], ['Pune', 'India'], ['Bengaluru', 'India'], ['Kolkata', 'India'], ['Surat', 'India'], ['Jaipur', 'India'], ['Chennai', 'India'], ['Ahmedabad', 'India'], ['New Delhi', 'India'], ['Quebec City', 'Canada'], ['Toronto', 'Canada'], ['Winnipeg', 'Canada'], ['Vancouver', 'Canada'], ['Ottawa', 'Canada'], ['Edmonton', 'Canada'], ['Montreal', 'Canada'], ['Calgary', 'Canada'], ['Victoria', 'Canada'], ['Regina', 'Canada'], ['Sydney', 'Australia'], ['Melbourne', 'Australia'], ['Adelaide', 'Australia'], ['Hobart', 'Australia'], ['Gold Coast', 'Australia'], ['Perth', 'Australia'], ['Brisbane', 'Australia'], ['Canberra', 'Australia'], ['Cairns', 'Australia'], ['Darwin', 'Australia'], ['London', 'United Kingdom'], ['Liverpool', 'United Kingdom'], ['Cardiff', 'United Kingdom'], ['Bristol', 'United Kingdom'], ['Manchester', 'United Kingdom'], ['Birmingham', 'United Kingdom'], ['Edinburgh', 'United Kingdom'], ['City of London', 'United Kingdom'], ['Glagow', 'United Kingdom'], ['Belfast', 'United Kingdom'], ['Rio de Janeiro', 'Brazil'], ['Manaus', 'Brazil'], ['Belo Horizonte', 'Brazil'], ['Salvador', 'Brazil'], ['Fortaleza', 'Brazil'], ['Brasilia', 'Brazil'], ['Sao Paulo', 'Brazil'], ['Curitiba', 'Brazil'], ['Recife', 'Brazil'], ['Natal', 'Brazil'], ['Tokyo', 'Japan'], ['Sapporo', 'Japan'], ['Hiroshima', 'Japan'], ['Kyoto', 'Japan'], ['Yokohama', 'Japan'], ['Kobe', 'Japan'], ['Osaka', 'Japan'], ['Fukuoka', 'Japan'], ['Nara', 'Japan'], ['Nagoya', 'Japan'], ['Moscow', 'Russia'], ['Kazan', 'Russia'], ['Saint Petersburg', 'Russia'], ['Ufa', 'Russia'], ['Nizhny Novgorod', 'Russia'], ['Yekaterinburg', 'Russia'], ['Novosibirsk', 'Russia'], ['Chelyabinsk', 'Russia'], ['Sochi', 'Russia'], ['Samara', 'Russia'], ['Bamako', 'Mali'], ['Timbuktu', 'Mali'], ['Douala', 'Cameroon'], ['Yaounde', 'Cameroon'], ['Garoua', 'Cameroon'], ['Cape Town', 'South Africa'], ['Gqeberha', 'South Africa'], ['East London', 'South Africa'], ['Johannesburg', 'South Africa'], ['Pretoria', 'South Africa'], ['Pietermaritzburg', 'South Africa'], ['Durban', 'South Africa'], ['Bloemfontein', 'South Africa'], ['Kimberley', 'South Africa'], ['Soweto', 'South Africa']]

def datagenerate(records, headers):
    fake = Faker('en_US')
    fake1 = Faker('en_GB')   # To generate phone numbers
    with open("Random_data5.csv", 'wt') as csvFile:
        writer = csv.DictWriter(csvFile, fieldnames=headers)
        writer.writeheader()
        for i in range(records):
            full_name = fake.name()
            FLname = full_name.split(" ")
            Fname = FLname[0]
            Lname = FLname[1]
            domain_name = "@toyDomain.com"
            orderId = Fname +"."+ Lname + domain_name
            location = random.randint(0,len(lst)-1)
            writer.writerow({
                    "Order Id" : orderId,
                    "Name": fake.name(),
                    "Order Date" : fake.date(pattern="%d-%m-%Y", end_datetime=datetime.date(2000, 1,1)),
                    "Payment Type" : random.choice(['card', 'Internet Banking', 'UPI', 'Wallet']),
                    "City": lst[location][0],
                    "Country" : lst[location][1] 
                    })
    
if __name__ == '__main__':
    records = 10000
    headers = ["Order Id", "Name", "Order Date", "Payment Type", "City", "Country"]
    datagenerate(records, headers)
    print("CSV generation complete!")
