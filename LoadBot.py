import os
from InstagramDownloader import Login

username = os.environ['INSTA_USERNAME']
password = os.environ['INSTA_PASSWORD']
admins = [username, "wuiyang_tan"] # include admin here
promote_message = "This bot is on test"

print("startup")
Login(username, password)
print("completed")
