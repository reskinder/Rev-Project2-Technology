import random
  
transactionID= list(range(100,99999))

randTransaction = random.sample(transactionID, k=10000)

print(randTransaction)












# sampleList = [100, 200, 300, 400, 500]
  
# randomList = random.choices(
#   sampleList, weights=(10, 20, 30, 40, 50), k=5)
  
# print(randomList)