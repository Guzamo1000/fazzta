import threading
import json
import requests

url_data = {}

def crawl_and_save(url):
    # Thực hiện request để lấy dữ liệu từ API
    response = requests.get(url)
    data = response.json()
    
    # Lưu trữ dữ liệu json của từng URL
    url_data[url] = data
    
    # Kết nối cơ sở dữ liệu
    mycursor = mydb.cursor()
    
    # Thực hiện lưu dữ liệu vào cơ sở dữ liệu
    for item in data:
        sql = "INSERT INTO table_name (column1, column2, column3) VALUES (%s, %s, %s)"
        val = (item['value1'], item['value2'], item['value3'])
        mycursor.execute(sql, val)
    
    # Lưu thay đổi vào cơ sở dữ liệu
    mydb.commit()

urls = ['https://api.example.com/data1', 'https://api.example.com/data2', 'https://api.example.com/data3']

threads = []

# Khởi tạo thread cho mỗi URL
for url in urls:
    t = threading.Thread(target=crawl_and_save, args=(url,))
    threads.append(t)
    t.start()

# Chờ cho tất cả các thread thực hiện xong công việc
for t in threads:
    t.join()

# Lưu trữ dữ liệu của từng URL vào các file json tương ứng
for url, data in url_data.items():
    with open(url.split("/")[-1] + '.json', 'w') as file:
        json.dump(data, file)
