import pandas as pd
import requests
import mysql.connector
import concurrent.futures
import pymysql
import time
import threading
import json
import couchdb
cnx = mysql.connector.connect(user='root', password='258000',
                              host='localhost',
                              database='fazzta')
df=pd.read_sql("SELECT id, form, lemma FROM nomen", cnx)
noun=list(set(df['form']))
print(type(noun))
couch = couchdb.Server('http://admin:123456@localhost:5984')
try:
    db=couch['fazzta2']
except:
    db=couch.create("fazzta2")

genders_ls=[]
# hàm crawl
def crawl(noun):
    print(f"noun: {noun}")
    if noun=="": 
        print("null")
        return
    url="https://www.qmez.de:8444/v1/scanner/es/s?w="+str(noun)
    response=requests.get(url)
    if response.text=='':
        print("null")
        return
    js_gender=response.json()
    request={}
    request['api']=js_gender
    request['link']= url
    time.sleep(5)
    # print()
    genders_ls.append(js_gender)
    db.save(request)
    # gender=js_gender['word']
    # cursor=cnx.cursor()
    # cursor.execute(f"insert into request(url,gender,noun) values ('https://www.qmez.de:8444/v1/scanner/es/s?w={gender}','{gender}','{noun}')")
    # cnx.commit()
    print(f"success ")
    # return  js_gender
thread=[]
o=0
for n in noun:
    print(o)
    o+=1
    t=threading.Thread(target=crawl,args=(n,))
    thread.append(t)
    t.start()

for t in thread:
    t.join()

with open("genders.json",'w') as f:
    json.dump(genders_ls,f)