from bs4 import BeautifulSoup
import requests
import json
import pymysql
import re
from datetime import datetime, timedelta


now = datetime.now().date()
dateFrom = str(now - timedelta(days=1))

try:
    connection = pymysql.connect()
    with connection.cursor() as cursor:
        cursor.execute('SELECT sku_id, link FROM main_aliexpress1 WHERE is_published = 1')
        id_mains_sku = dict(cursor.fetchall())
except:
    print('Не получилось подключиться к БД')
finally:
    connection.close()

def get_num_page():
    url = 'https://aliexpress.ru/aer-webapi/v1/search'
    headers = {
        'authority': 'aliexpress.ru',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="96", "Google Chrome";v="96"',
        'sec-ch-ua-mobile': '?0',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36',
        'sec-ch-ua-platform': '"Windows"',
        'content-type': 'application/json',
        'accept': '*/*',
        'origin': 'https://aliexpress.ru',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'referer': 'https://aliexpress.ru/wholesale?CatId=&page=2&SearchText=%D1%82%D0%B5%D1%80%D0%BC%D0%BE%D0%BF%D0%B0%D1%81%D1%82%D0%B0',
        'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    }
    data2= '{"catId":"","searchText":"термопаста","page":1}'
    data2 = data2.encode('utf-8')
    req = requests.post('https://aliexpress.ru/aer-webapi/v1/search', headers=headers, data=data2)
    src = req.text
    with open('date_list.html', 'w', encoding='utf-8') as file:
        file.write(src)
    soup = BeautifulSoup(src, 'html.parser')
    site_json = json.loads(soup.text)
    max_num_page = int(site_json['data']['pagination']['totalPages'])
    return max_num_page

def scrapungALI(count):
    url = 'https://aliexpress.ru/aer-webapi/v1/search'
    headers = {
        'authority': 'aliexpress.ru',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="96", "Google Chrome";v="96"',
        'sec-ch-ua-mobile': '?0',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36',
        'sec-ch-ua-platform': '"Windows"',
        'content-type': 'application/json',
        'accept': '*/*',
        'origin': 'https://aliexpress.ru',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'referer': 'https://aliexpress.ru/wholesale?CatId=&page=2&SearchText=%D1%82%D0%B5%D1%80%D0%BC%D0%BE%D0%BF%D0%B0%D1%81%D1%82%D0%B0',
        'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    }
    data_list = []
    try:
        for z in range(1,count):
            data2= '{"catId":"","searchText":"термопаста","page":'+str(z)+'}'
            data2 = data2.encode('utf-8')
            req = requests.post('https://aliexpress.ru/aer-webapi/v1/search', headers=headers, data=data2)
            src = req.text
            soup = BeautifulSoup(src, 'html.parser')
            site_json = json.loads(soup.text)
            if site_json['data']['breadcrumbs']['breadcrumbs'] != []:
                data_list.append(site_json)
        with open('date_list_ALI_urls.json', 'w', encoding='utf-8') as file:
            json.dump(data_list, file, indent=4, ensure_ascii=False)
        print('Я закончил сбор информации')

    except Exception:
        print('Кончились страницы')
    try:
        with open('date_list_ALI_urls.json', encoding='utf-8') as file:
            file_content = file.read()
        soup = BeautifulSoup(file_content, 'html.parser')
        site_json = json.loads(soup.text)
        all_info_ali = []
        numbers=1
        for page in site_json:
            for i in page['data']['productsFeed']['products']:
                flag = True
                for key,value in id_mains_sku.items():
                    if i['id'] == key:
                        all_info_ali.append({
                            'number': numbers,
                            'name': i['productTitle'],
                            'url': i['productUrl'],
                            'id': i['id'],
                            'rating': i['rating'],
                            'price': float(re.sub("[^0-9+,]", "", i['finalPrice']).replace(',','.')),
                            'old_price': float(re.sub("[^0-9+,]", "",i['fullPrice']).replace(',','.')) if  re.sub("[^0-9+,]", "",i['fullPrice']) != "" else float(re.sub("[^0-9+,]", "", i['finalPrice']).replace(',','.')),
                            'url_img': i['imgSrc'],
                            'low_rating': get_ratings_and_low_price(key)[1],
                            'count_rating': get_ratings_and_low_price(key)[0],
                            'stock': int(count_goods(value))
                        })
                        flag = False
                if flag:
                    all_info_ali.append({
                        'number': numbers,
                        'name': i['productTitle'],
                        'url': i['productUrl'],
                        'id': i['id'],
                        'rating': i['rating'],
                        'price': float(re.sub("[^0-9+,]", "", i['finalPrice']).replace(',','.')),
                        'old_price': float(re.sub("[^0-9+,]", "",i['fullPrice']).replace(',','.')) if  re.sub("[^0-9+,]", "",i['fullPrice']) != "" else float(re.sub("[^0-9+,]", "", i['finalPrice']).replace(',','.')),
                        'url_img': i['imgSrc'],
                    })
                numbers+=1
        with open('data_list_ALI.json', 'w', encoding='utf-8') as file:
            json.dump(all_info_ali, file, indent=4, ensure_ascii=False)
        print('Закончил ')
    except Exception as ex:
        print('DONT')
        print(ex)

def get_ratings_and_low_price(id_ali_goods):
    headers = {
        'authority': 'aliexpress.ru',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="96", "Google Chrome";v="96"',
        'sec-ch-ua-mobile': '?0',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36',
        'sec-ch-ua-platform': '"Windows"',
        'content-type': 'application/json',
        'accept': '*/*',
        'origin': 'https://aliexpress.ru',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'referer': 'https://aliexpress.ru/item/1005003036320874.html?item_id=1005003036320874&sku_id=12000023369577260&spm=a2g2w.productlist.0.0.72af3433H3chPK',
        'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    }
    data = '{"productId":'+str(id_ali_goods)+',"starFilter":"all","sort":"default","pageSize":10,"translate":true,"local":false}'
    response = requests.post('https://aliexpress.ru/aer-api/v1/review/filters', headers=headers, data=data)
    src = response.text
    with open('date_raiting.html', 'w', encoding='utf-8') as file:
        file.write(src)
    soup = BeautifulSoup(src, 'html.parser')
    site_json = json.loads(soup.text)
    low_rating = 0
    ratings = site_json['reviewInfo']['count']
    if site_json['reviewInfo']['count'] == 0:
        low_rating = 0
    else:
        for i in site_json['reviewInfo']['reviews']:
            if i['rating']<=3:
                low_rating+=1
    return ratings, low_rating

def count_goods(url_ali):
    url = url_ali
    headers = {
        'accept': '*/*',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36'
    }
    req = requests.post(url=url, headers=headers)
    src = req.text
    with open('goods.html', 'w', encoding='utf-8') as file:
        file.write(src)
    soup = BeautifulSoup(src, 'html.parser')
    try:
        gooods = soup.find(class_= 'Product_Quantity__countText__1bkyi')
        stock = re.sub("[^0-9]", "", gooods.text)
        return stock
    except:
        return 0

def insert_DB():
    connection = pymysql.connect()
    with open('data_list_ALI.json', encoding='utf-8') as file:
        file_content = file.read()
    soup = BeautifulSoup(file_content, 'html.parser')
    site_json = json.loads(soup.text)
    for i in id_mains_sku.keys():
        # try:
        #     with connection.cursor() as cursor:
        #         cursor.execute(f"CREATE TABLE aliexpress1_{i}(ID int NOT NULL AUTO_INCREMENT, date varchar(25) NOT NULL,  price varchar(25) NOT NULL, old_price varchar(25) NOT NULL, sale varchar(25) NOT NULL, rating varchar(25) NOT NULL, feedbacks varchar(25) NOT NULL, oonps varchar(25) NOT NULL, stock varchar(25) NOT NULL, nal varchar(25) NOT NULL, image varchar(255) NOT NULL, popul varchar(25) NOT NULL,PRIMARY KEY (ID))")
        # except Exception as ex:
        #         print('Не получилось создать')
        #         print(ex)
        try:
            with connection.cursor() as cursor:
                cursor.execute(f'SELECT date,price FROM aliexpress1_{i}')
                date_last = dict(cursor.fetchall())
            if dateFrom not in date_last:
                for z in site_json:
                    if i == z['id']:
                        with connection.cursor() as cursor:
                            cursor.execute(
                                f"INSERT INTO aliexpress1_{i} (date,price,old_price,sale,rating,feedbacks,oonps, stock, nal, image,popul) VALUES ('{dateFrom}', {z['price']}, {z['old_price']}, 0, {z['rating']}, {z['count_rating']}, {z['low_rating']},{z['stock']},0,'{z['url_img']}',{z['number']})")
            else:
                continue
            connection.commit()
        except Exception as ex:
            print('Не получилось добавить')
            print(ex)
        finally:
            connection.close()

if __name__ == "__main__":
    get_num_page()
    scrapungALI(get_num_page())
    insert_DB()

