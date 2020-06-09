import os
import datetime
import Language
import pymongo
import requests

from Api import InstagramAPI

# choosing mongoDB as it stores more data compared with postgres
# https://medium.com/@shivam270295/estimating-average-document-size-in-a-mongodb-collection-953b0788fac0
# instagram redo https://stackoverflow.com/a/60744028

# Single MongoDB instance

class MongoDB(object):
    def __init__(self):
        client = pymongo.MongoClient(os.environ["MONGODB_URI"])
        self.db = client.get_default_database()

SingleMongoDB = MongoDB()

# Storage class for Days, Users and Requests data and statistics

class Storage(object):
    def __init__(self):
        self.DEFAULT_PRIORITY = 1
        self.init_db()

    def init_db(self):
        self.db = SingleMongoDB.db
        self.users = self.db["users"]
        self.days = self.db["days"]

    # DAY STATS

    def get_day_download(self, day = None):
        date = None
        if day is None:
            date = datetime.date.today()
        else:
            day = day.split("-")
            day.reverse()
            day = "-".join(day)
            date = datetime.date.isoformat(day)
        date = datetime.datetime.combine(date, datetime.time())
        date_res = self.days.find_one({ "date": date })
        return date_res["counts"] if date_res is not None else 0

    def day_add_download(self):
        date = datetime.datetime.combine(datetime.date.today(), datetime.time())
        self.days.update_one({ "date": date }, { "$inc": { "counts": 1 } }, upsert = True)

    # USER DATA

    def format_userid(self, userid):
        return userid if isinstance(userid, int) else int(userid) if userid.isdigit() else 0

    def create_user(self, userid, username):
        userData = {
            "userid": userid,
            "username": username,
            "priority": self.DEFAULT_PRIORITY,
            "latest_item_time": 0,
            "downloaded_from": []
        }
        user = self.users.insert_one(userData)
        userData["_id"] = user.inserted_id
        return userData

    def modify_user(self, search_query, modify_query, none_insert = False):
        user = self.users.find_one_and_update(search_query, modify_query, upsert = none_insert, return_document = pymongo.ReturnDocument.AFTER)
        return user
    
    def internal_get_user(self, userid, create = False, username = ""):
        user = None

        if userid is not None and userid != "":
            user = self.users.find_one({ "userid": userid })
        
        if user is None:
            user = self.users.find_one({ "username": username })

        if user is None and create and username != "":
            return self.create_user(userid, username)
        
        if user is not None:
            set_query = {}
            need_set = False

            if user["userid"] == "" and userid != "":
                set_query["userid"] = userid
                need_set = True
            if user["username"] == "" and username != "" and username != "@UNKNOWN@":
                set_query["username"] = username
                need_set = True
            
            if need_set:
                user = self.users.find_one_and_update({ "_id": user["_id"] }, { "$set": set_query }, return_document = pymongo.ReturnDocument.AFTER)
        return user

    def get_user(self, userid):
        userid = self.format_userid(userid)
        return self.internal_get_user(userid)

    def user_add_download(self, userid, username, downloaded_from):
        userid = self.format_userid(userid)
        user = self.internal_get_user(userid, create = True, username = username)
        if user == None:
            return False

        # first add in { username, downloads: 0 } if array does not have username
        self.users.update_one({
            "userid": userid,
            "downloaded_from.username": { "$ne": downloaded_from }
        }, {
            "$push": { 
                "downloaded_from": { "username": downloaded_from, "downloads": 0 }
            }
        })

        # then increase by 1
        self.users.update_one({
            "userid": userid,
            "downloaded_from.username": downloaded_from
        }, {
            "$inc": { "downloaded_from.$.downloads": 1 }
        })

        # add day download count
        self.day_add_download()

        return True

    def check_user(self, username, userid = ""):
        userid = self.format_userid(userid)
        return self.internal_get_user(userid, create = True, username = username)

    def user_set_itemtime(self, userid, username, item_time):
        userid = self.format_userid(userid)
        user = self.check_user(username, userid)
        self.modify_user({ "_id": user["_id"] }, {"$set": { "latest_item_time": item_time } })

    def upgrade_priority(self, username):
        user = self.check_user(username)
        user = self.modify_user({ "_id": user["_id"] }, {"$inc": { "priority": 1 }})
        return user["priority"]

    def downgrade_priority(self, username):
        user = self.check_user(username)
        user = self.modify_user({ "_id": user["_id"] }, {"$inc": { "priority": -1 }})
        return user["priority"]

    # USER STATS

    def requested_query(self, username = "", top_amount = 5):
        # pipeline for top 5 most requested account: unwind, group, sort and limit
        # pipeline for top 5 most requestors for specific requested account: unwind, match, sort, limit and group
        has_username = username != "" and username is not None

        aggregate_pipe = [ { "$unwind": { "path": "$downloaded_from" } } ]
        group_pipe = {
            "_id": "$downloaded_from.username",
            "total": { "$sum": "$downloaded_from.downloads" }
        }
        sort_pipe = {}

        if has_username:
            group_pipe["requestors"] = {
                "$push": {
                    "username": "$username", 
                    "downloads": "$downloaded_from.downloads"
                }
            }
            sort_pipe["downloaded_from.downloads"] = -1
            aggregate_pipe.append({ "$match": { "downloaded_from.username": username } })
        else:
            sort_pipe["total"] = -1
            aggregate_pipe.append({ "$group": group_pipe })
        
        aggregate_pipe.append({ "$sort": sort_pipe })

        aggregate_pipe.append({ "$limit": top_amount })

        if has_username:
            aggregate_pipe.append({ "$group": group_pipe })
        
        return self.users.aggregate(aggregate_pipe)

    def format_download_text(self, array, username_key, total_key):
        index = 1
        output = ""
        
        for item in array:
            output += "\r\n{i}. @{u} ({c} downloads)".format(i = index, u = item[username_key], c = item[total_key])
            index += 1

        return output

    def format_output(self, output, extra_info, username):
        if extra_info == "":
            if username == "" or username is None:
                return Language.get_text("admin.nodata")
            return "No information found for account @{u}".format(u = username)
        return output + extra_info

    def get_post_owner_info(self, username = "", top_amount = 5):
        has_username = username != "" and username is not None
        results = self.requested_query(username = username, top_amount = top_amount)
        output = "Top {c} ".format(c = top_amount)
        extra_info = ""

        if has_username:
            total = 0
            for requested_user in results:
                total = requested_user["total"]
                extra_info += self.format_download_text(requested_user["requestors"], "username", "downloads")
            output += "downloaders for post account @{u} (total of {t} downloads)".format(u = username, t = total)
        else:
            output += "downloaded post accounts"
            extra_info += self.format_download_text(results, "_id", "total")
        
        return self.format_output(output, extra_info, username)

    def get_post_downloader_info(self, username = "", top_amount = 5):
        has_username = username != "" and username is not None
        aggregate_pipe = []
        output = "Top {c} ".format(c = top_amount)
        results = None
        key = ""

        # add filter username pipeline FIRST if username exist
        if has_username:
            aggregate_pipe.append( { "$match": { "username": username } } )
        
        aggregate_pipe += [
            { "$addFields": { "total": { "$sum": "$downloaded_from.downloads" } } },
            { "$sort": { "total": -1 } },
            { "$limit": top_amount }
        ]
        
        if has_username:
            key = "downloads"
            output += "downloaded post account for downloader @{u} ".format(u = username)
        else:
            key = "total"
            output += "downloaders"
            
        results = self.users.aggregate(aggregate_pipe)
        
        if has_username:
            total = 0
            for result in results:
                total = result["total"]
                results = sorted(result["downloaded_from"], key = lambda dl: dl["downloads"], reverse = True)[:top_amount]
            output += "(total of {t} downloads)".format(t = total)
        
        extra_info = self.format_download_text(results, "username", key)

        return self.format_output(output, extra_info, username)

    # REQUEST DATA
    def requested_add_request(self, username, requested_by_userid):
        return None
        # requested = self.get_requested_unsafe(username)
        # requestor = self.add_get_requestor(requested, str(requested_by_userid))
        # self.lock.acquire()
        # requestor["requested"] += 1
        # self.lock.release()

# Separated API storage class
# TODO: implement encryption

class APIStorage(object):
    def __init__(self, session_id):
        self.sessions = SingleMongoDB.db["sessions"]
        self.session_id = session_id
        self.username = ""
        self.password = ""
    
    def save(self, instaAPI):
        output_data = {
            "session_id": self.session_id,
            "device_id": instaAPI.device_id,
            "uuid": instaAPI.uuid,
            "isLoggedIn": instaAPI.isLoggedIn,
            "username_id": instaAPI.username_id,
            "rank_token": instaAPI.rank_token,
            "token": instaAPI.token,
            "cookies": self.extract_cookies(instaAPI.s.cookies)
        }
        self.sessions.update_one({ 'session_id': self.session_id },  { "$set": output_data }, upsert=True)

    def load(self, username = "", password = ""):
        self.username = username if self.username == "" else self.username
        self.password = password if self.password == "" else self.password
        instaAPI = InstagramAPI(self.username)

        output_data = self.sessions.find_one({ 'session_id': self.session_id })
        if output_data is None:
            instaAPI.login(self.password)
            self.save(instaAPI)
        else:
            instaAPI.device_id = output_data['device_id']
            instaAPI.uuid = output_data["uuid"]
            instaAPI.isLoggedIn = output_data["isLoggedIn"]
            instaAPI.username_id = output_data["username_id"]
            instaAPI.rank_token = output_data["rank_token"]
            instaAPI.isLoggedIn = True
            instaAPI.token = output_data["token"]
            self.to_cookies(output_data['cookies'], instaAPI.s.cookies)
            instaAPI.s.headers.update({ 'Cookie2': '$Version=1',
                                        'Accept-Language': 'en-US',
                                        'Accept-Encoding': 'gzip, deflate',
                                        'Accept': '*/*',
                                        'Content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
                                        'Connection': 'close',
                                        'User-Agent': instaAPI.USER_AGENT })
        
        return instaAPI

    
    def extract_cookies(self, cookies):
        sim_cookies = []

        for cookie in cookies:
            sim_cookie = { 'name': cookie.name, 'value': cookie.value }

            if cookie.expires is not None:
                sim_cookie['expires'] = cookie.expires

            if 'HttpOnly' in cookie._rest:
                sim_cookie['HttpOnly'] = True

            sim_cookies.append(sim_cookie)

        return sim_cookies

    def to_cookies(self, sim_cookies, cookies):
        for sim_cookie in sim_cookies:
            expires = sim_cookie['expires'] if 'expires' in sim_cookie and sim_cookie['expires'] else None
            rest = { 'HttpOnly': None } if 'HttpOnly' in sim_cookie and sim_cookie['HttpOnly'] else None
            discard = sim_cookie['name'] == "urlgen"
            cookies.set(sim_cookie['name'], sim_cookie['value'], expires=expires, rest=rest, secure=True, domain='.instagram.com', discard=discard)


