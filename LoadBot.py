import os
from InstagramDownloader import Login

username = os.environ['INSTA_USERNAME']
password = os.environ['INSTA_PASSWORD']

print("startup")
Login(username, password)
print("completed")
