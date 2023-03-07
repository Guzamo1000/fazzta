import concurrent.futures
import requests
from bs4 import BeautifulSoup
import pymysql

# Hàm crawl và cập nhật gender sử dụng threading
def crawl_and_update_gender_threading():
    # Kết nối cơ sở dữ liệu
    conn = pymysql.connect(host='localhost', user='root', password='258000', database='fazzta')
    cursor = conn.cursor()

    # Truy vấn các danh từ trong bảng nomen
    query = "SELECT id, form, lemma FROM nomen"
    cursor.execute(query)
    results = cursor.fetchall()

    # Tạo một danh sách các URL cần crawl
    urls = []
    for result in results:
        url = result[1]
        urls.append((result[0], url))  # Lưu id và URL tương ứng

    # Hàm crawl một URL và trả về giới tính
    def crawl_url(url):
        #url là danh từ cần tìm giới tính
        
        print(f"Noun: {url}")
        response=requests.get("https://der-artikel.de/die/"+str(url)+".html")
        if response.status_code==404:
            response=requests.get("https://der-artikel.de/der/"+str(url)+".html")
            if response.status_code==404: 
                response=requests.get("https://der-artikel.de/das/"+str(url)+".html")
                if response.status_code==404:
                    print("does not exist")
                    return

        html=response.content
        soup=BeautifulSoup(html, "html.parser")
        h=soup.find("h3", class_="mb-5")
        gender_label=h.find("em").text.split(",")
            # gender_label=gender.text.split(",")
            # print
        print(f"*success: {gender_label[1]}")
        return gender_label[1]
            

    # Hàm cập nhật giới tính của một danh từ vào cơ sở dữ liệu
    def update_gender(noun_id, gender):
        query = f"UPDATE nomen SET gender = '{gender}' WHERE id = {noun_id}"
        cursor.execute(query)
        conn.commit()

    # Sử dụng ThreadPoolExecutor để tải dữ liệu từ các URL và cập nhật cơ sở dữ liệu
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Gửi các nhiệm vụ đến bộ xử lý đa nhiệm
        futures = [executor.submit(crawl_url, url) for id, url in urls]
        
        # Lấy kết quả và cập nhật cơ sở dữ liệu
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            gender = future.result()
            noun_id = urls[i][0]
            update_gender(noun_id, gender)

    # Đóng kết nối cơ sở dữ liệu
    cursor.close()
    conn.close()


crawl_and_update_gender_threading()