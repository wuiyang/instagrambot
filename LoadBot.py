import os
from InstagramDownloader import Login

username = os.environ['INSTA_USERNAME']
password = os.environ['INSTA_PASSWORD']
admins = [username] # include admin here
promote_message = "This bot is being run by u/floofygroup, follow them for updates"

Login(username, password, admins, promote_message)
