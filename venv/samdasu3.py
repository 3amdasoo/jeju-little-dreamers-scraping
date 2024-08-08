import requests
import pandas as pd
import time
import mysql.connector
from mysql.connector import errorcode
import hashlib
import random
import re

# MySQL 연결 정보
db_config = {
    'user': 'root',
    'password': 'wata1945',
    'host': 'localhost',
    'database': 'samdasu',
    'raise_on_warnings': True
}

# 다양한 User-Agent 헤더 설정
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/81.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/86.0.622.51"
]

# HTTP 요청 보내기 함수
def get_data(url):
    headers = {
        "User-Agent": random.choice(user_agents)
    }
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to retrieve data: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None

# 데이터베이스 연결
def connect_db():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        return conn, cursor
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
        return None, None

# 데이터 삽입 함수
def insert_data(conn, cursor, data):
    store_id = data['id']  # 실제로 사용되는 place의 id
    store_data = (
        store_id, 
        data['name'], 
        data['address'], 
        data['tel'], 
        data['category'], 
        data['latitude'], 
        data['longitude']
    )
    
    # 중복 항목 확인
    cursor.execute('SELECT COUNT(*) FROM store WHERE id = %s', (store_id,))
    if cursor.fetchone()[0] == 0:
        cursor.execute('''
            INSERT INTO store (id, name, address, phone, category, latitude, longitude)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        ''', store_data)

    menu_items = data['menu_info'].split('|') if isinstance(data['menu_info'], str) else []
    for item in menu_items:
        try:
            # 정규 표현식을 사용하여 가격을 추출하고 나머지를 이름으로 처리
            match = re.search(r'(.+)\s(\d{1,3}(,\d{3})*(\.\d+)?)(?=\s*$)', item)
            if match:
                name = match.group(1).strip()
                price = match.group(2).strip()
                # 메뉴 아이디 생성 (store_id와 menu_name의 해시값 사용)
                menu_id = hashlib.md5(f"{store_id}_{name}_{price}".encode()).hexdigest()
                menu_data = (menu_id, store_id, name, price)
                # 중복된 메뉴 항목을 삽입하기 전에 기존 메뉴 항목을 삭제
                cursor.execute('DELETE FROM menu WHERE id = %s', (menu_id,))
                cursor.execute('''
                    INSERT INTO menu (id, store_id, name, price)
                    VALUES (%s, %s, %s, %s)
                ''', menu_data)
            else:
                print(f"Skipping menu item due to parsing error: {item}")
        except ValueError:
            print(f"Skipping menu item due to parsing error: {item}")

    conn.commit()

# CSV 파일 읽기
df = pd.read_csv('C:/study/csv/제주특별자치도_아동급식카드가맹점현황_20240520.csv', encoding='cp949')

# 첫 100개 행만 선택
df = df.iloc[8202:8302]

# 기본 URL
base_url = "https://map.naver.com/p/api/search/allSearch?type=all&searchCoord=126.29921620000067%3B33.352623099999775&query="

# 데이터베이스 연결
conn, cursor = connect_db()
if conn is None or cursor is None:
    print("Failed to connect to the database")
else:
    for index, row in df.iterrows():
        # 가게 이름 + 주소 가져오기
        searchName = row['가맹점명']
        searchAddress = row['주소']

        # 검색어 예시 (여기서 필요한 검색어로 변경 가능)
        search_query = searchName + " " + searchAddress

        # 요청 URL 생성
        request_url = base_url + search_query

        # 재시도 로직 추가
        max_retries = 5
        retry_delay = 5  # seconds

        for attempt in range(max_retries):
            data = get_data(request_url)
            if data:
                break
            else:
                print(f"Retrying in 5 ~8 seconds... (Attempt {attempt + 1} of {max_retries})")
                time.sleep(random.uniform(4, 7))

        # 데이터가 성공적으로 받아진 경우 처리
        if data:
            # 필요한 정보 추출
            try:
                place_info = data.get('result', {}).get('place', {}).get('list', [])
            except Exception as e:
                place_info = []
                print(f"Error extracting place info: {e}")

            if place_info:
                for item in place_info:
                    try:
                        place_id = item.get('id')  # 수정된 부분: list 내부의 id 사용
                    except Exception as e:
                        place_id = f"Error: {e}"

                    try:
                        name = item.get('name')
                    except Exception as e:
                        name = f"Error: {e}"

                    try:
                        tel = item.get('tel', 'N/A')
                    except Exception as e:
                        tel = f"Error: {e}"

                    try:
                        category = ', '.join(item.get('category', [])) if isinstance(item.get('category', []), list) else item.get('category', 'N/A')
                    except Exception as e:
                        category = f"Error: {e}"

                    try:
                        status = item.get('businessStatus', {}).get('status', {}).get('text', 'N/A')
                    except Exception as e:
                        status = f"Error: {e}"

                    try:
                        menu_info = item.get('menuInfo', 'N/A')
                    except Exception as e:
                        menu_info = f"Error: {e}"

                    try:
                        address = item.get('address', 'N/A')
                    except Exception as e:
                        address = f"Error: {e}"

                    try:
                        boundary = data.get('result', {}).get('place', {}).get('boundary', [])
                        longitude = boundary[0] if len(boundary) > 0 else 'N/A'
                        latitude = boundary[1] if len(boundary) > 1 else 'N/A'
                    except Exception as e:
                        latitude = longitude = f"Error: {e}"

                    print(f"ID: {place_id}")
                    print(f"Name: {name}")
                    print(f"Tel: {tel}")
                    print(f"Category: {category}")
                    print(f"Status: {status}")
                    print(f"Menu Info: {menu_info}")
                    print(f"Latitude: {latitude}")  # 위도
                    print(f"Longitude: {longitude}")  # 경도
                    print(f"Address: {address}")

                    store_data = {
                        'id': place_id,  # 수정된 부분: store_data에 place_id 추가
                        'name': name,
                        'address': address,
                        'tel': tel,
                        'category': category,
                        'latitude': latitude,
                        'longitude': longitude,
                        'menu_info': menu_info
                    }

                    insert_data(conn, cursor, store_data)
                    print(f"Inserted data for store: {name}")
                    print("-" * 50)
                    # 요청 간 지연 추가
                    time.sleep(random.uniform(5, 8))
            else:
                print("No place information found.")
        else:
            print("Failed to retrieve data after multiple attempts.")

    cursor.close()
    conn.close()
