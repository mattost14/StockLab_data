from Functions.functions import getSum 
from Firestore.uploadToFirestore import uploadToFirestore

def sayHello():
    print('Hello')

print(getSum(2,4))

uploadToFirestore('a','b')