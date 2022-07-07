import random



validity = ["Y","N"]
test = random.choices(validity, weights=(95,5), k=1)
print(test)

list = ["authentication failure", "invalid method", "insufficient funds", "processor declinded", 
 "account suspended", "expired card", "invalid address", "invalid cvv"]

if "N" in test:
    choice = random.choice(list)
    print(choice)
if "Y" in test:
    print("Successful payment")



# validity = ["Y","N","Y","Y","Y","Y","Y","Y","Y","Y"]
#test = random.choice(validity)
# print(test)

# list = ["authentication failure", "invalid method", "insufficient funds", "processor declinded", 
#  "account suspended", "expired card", "invalid address", "invalid cvv"]

# if(test=="N"):
#     choice = random.choice(list)
#     print(choice)
# if(test=="Y"):
#     print("Successful payment")