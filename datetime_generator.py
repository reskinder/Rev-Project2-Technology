#
# Datetime Generator
#

# Imports
from faker import Faker

# Fake object
fake = Faker()

# Datetime
dateTime = fake.date_time_between(start_date='-2y', end_date='now')

print("Datetime:", dateTime)