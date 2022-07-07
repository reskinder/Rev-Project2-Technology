#
# Website Data Generator
#

# Imports
import pandas as pd
import random

# USING PANDAS TO READ CSV FILE
df = pd.read_csv("ecommerce_websites.csv")

# List of countries
countriesList = ["USA", "Germany", "Brazil", "Australia", "Japan", "China", "India", "Mexico", 
                "Canada", "UK", "Russia", "Mali", "Cameroon", "South Africa", "Nigeria"]

# Extracting sites by country into dict
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

# Randomly choosing site based on country (will be changed later to match Delia's random country generator)
countryPick = random.choice(countriesList)

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

# Print chosen country and site
print("Country:", countryPick)
print("Site:", sitePick)


# Generating fake datetime