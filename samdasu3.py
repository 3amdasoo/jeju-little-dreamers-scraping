import os
import requests
import pandas as pd
import time
import mysql.connector
from mysql.connector import errorcode
import hashlib
import random
import re
from collections import deque

# 실패한 요소를 기록하는 파일 경로
failed_requests_file = './failed_requests.txt'
progress_file = './progress.txt'  # 크롤링 진행 상태를 저장하는 파일

# MySQL 연결 정보
db_config = {
    'user': 'junpark',
    'password': 'Dnjswns3428!',
    'host': 'junfirstdbinstance.che2useq0r4t.ap-northeast-2.rds.amazonaws.com',
    'database': 'samdasu',
    'raise_on_warnings': True,
    'charset': 'utf8mb4'  # 이모지 등 UTF-8을 처리하기 위해 utf8mb4 사용
}

# 다양한 User-Agent 헤더 설정
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/81.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/86.0.622.51",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Linux; Android 11; SM-G991U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Mobile Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) Gecko/20100101 Firefox/90.0",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:79.0) Gecko/20100101 Firefox/79.0",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:91.0) Gecko/20100101 Firefox/91.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 13_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:87.0) Gecko/20100101 Firefox/87.0",
    "Mozilla/5.0 (Linux; Android 11; SM-A205U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.164 Safari/537.36",
    "Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_2_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36"
]


# 진행 상태를 저장하는 함수
def save_progress(index):
    with open(progress_file, 'w') as f:
        f.write(str(index))

# 저장된 진행 상태를 불러오는 함수
def load_progress():
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            return int(f.read())
    return 1  # 파일이 없으면 1에서 시작

# 진행 상태 파일을 삭제하는 함수
def clear_progress():
    if os.path.exists(progress_file):
        os.remove(progress_file)

# 실패한 요청을 기록하는 함수
def log_failed_request(search_query):
    with open(failed_requests_file, 'a') as f:
        f.write(f"{search_query}\n")

# 실패한 요청 파일을 큐에 로드하는 함수
def load_failed_requests():
    queue = deque()
    if os.path.exists(failed_requests_file):
        with open(failed_requests_file, 'r') as f:
            for line in f:
                queue.append(line.strip())
    return queue

# 큐에서 실패한 요청을 파일에 다시 기록하는 함수
def save_failed_requests(queue):
    with open(failed_requests_file, 'w') as f:
        for request in queue:
            f.write(f"{request}\n")

# 실패한 요청을 다시 크롤링하는 함수
def retry_failed_requests():
    queue = load_failed_requests()

    if not queue:
        print("No failed requests found.")
        return

    # 데이터베이스 연결
    conn, cursor = connect_db()
    if conn is None or cursor is None:
        print("Failed to connect to the database")
        return

    try:
        while queue:
            search_query = queue.popleft()  # 큐에서 첫 번째 요청을 가져옴
            request_url = base_url + search_query

            max_retries = 5
            data = get_data(request_url)

            if not data:
                for attempt in range(max_retries):
                    retry_delay = random.uniform(2, 4)
                    data = get_data(request_url)
                    if data:
                        break
                    else:
                        print(f"Retrying failed request in 1 ~ 3 seconds... (Attempt {attempt + 1} of {max_retries})")
                        time.sleep(retry_delay)

            if not data:
                # 데이터를 가져오지 못한 경우, 다음 요청으로 넘어가기 위해 continue
                print(f"No data for {search_query}, skipping.")
                continue

            if data and data.get('result'):
                place_info = data.get('result', {}).get('place', {}).get('list', [])
                if place_info:
                    for item in place_info:
                        place_id = item.get('id', "Error")
                        name = item.get('name', "Error")
                        tel = item.get('tel', 'N/A')
                        category = ', '.join(item.get('category', [])) if isinstance(item.get('category', []), list) else item.get('category', 'N/A')
                        menu_info = item.get('menuInfo', 'N/A')
                        address = item.get('address', 'N/A')
                        boundary = data.get('result', {}).get('place', {}).get('boundary', [])
                        longitude = boundary[0] if len(boundary) > 0 else 'N/A'
                        latitude = boundary[1] if len(boundary) > 1 else 'N/A'

                        store_data = {
                            'id': place_id,
                            'name': name,
                            'address': address,
                            'tel': tel,
                            'category': category,
                            'latitude': latitude,
                            'longitude': longitude,
                            'menu_info': menu_info
                        }

                        insert_data(conn, cursor, store_data)
                        print(f"Inserted data for failed request: {name}")
                        print("-" * 50)

            # 남아있는 요청을 계속 파일에 저장 (큐의 상태를 기록)
            save_failed_requests(queue)

    except Exception as e:
        print(f"Error retrying failed requests: {e}")
    finally:
        cursor.close()
        conn.close()

# HTTP 요청 보내기 함수
def get_data(url):
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Sec-Ch-Ua": '"Chromium";v="126", "Not A(Brand";v="8", "Google Chrome";v="126"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "User-Agent": random.choice(user_agents),
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    }

    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            try:
                return response.json()  # 유효한 JSON 응답일 경우만 반환
            except ValueError:
                print(f"Invalid JSON response from {url}")
                return None
        else:
            print(f"Failed to retrieve data: {response.status_code}")
            time.sleep(random.uniform(1,3))
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
    store_id = data['id']
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
            match = re.search(r'(.+)\s(\d{1,3}(,\d{3})*(\.\d+)?)(?=\s*$)', item)
            if match:
                name = match.group(1).strip()
                price = match.group(2).strip()
                menu_id = hashlib.md5(f"{store_id}_{name}_{price}".encode()).hexdigest()
                menu_data = (menu_id, store_id, name, price)
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
df = pd.read_csv('제주특별자치도_아동급식카드가맹점현황_20240520.csv', encoding='cp949')

# 기본 URL
base_url = "https://map.naver.com/p/api/search/allSearch?type=all&searchCoord=126.29921620000067%3B33.352623099999775&query="

# 크롤링 시작점 불러오기
start_index = load_progress()

# df에서 시작 지점부터 끝까지 선택
df = df.iloc[start_index:]

# 데이터베이스 연결
conn, cursor = connect_db()
if conn is None or cursor is None:
    print("Failed to connect to the database")
else:
    try:
        for index, row in df.iterrows():
            try:
                # 가게 이름 + 주소 가져오기
                searchName = row['가맹점명']
                searchAddress = row['주소']

                search_query = searchName + " " + searchAddress
                request_url = base_url + search_query

                max_retries = 5
                data = get_data(request_url)

                # data가 None일 경우 다음 검색어로 넘어감
                if data is None:
                    print(f"No data for {search_query}, skipping.")
                    save_progress(index)  # 진행 상황 저장
                    continue  # 데이터를 가져오지 못한 경우 건너뛰고 다음 검색어 처리

                # data가 유효하지 않거나 result가 없을 때도 건너뜀
                if not data.get('result'):
                    print(f"No result for {search_query}, skipping.")
                    save_progress(index)  # 진행 상황 저장
                    continue  # 데이터를 가져오지 못한 경우 건너뛰고 다음 검색어 처리

                place_info = data.get('result', {}).get('place', {}).get('list', [])
                if place_info:
                    for item in place_info:
                        place_id = item.get('id', "Error")
                        name = item.get('name', "Error")
                        tel = item.get('tel', 'N/A')
                        category = ', '.join(item.get('category', [])) if isinstance(item.get('category', []), list) else item.get('category', 'N/A')
                        menu_info = item.get('menuInfo', 'N/A')
                        address = item.get('address', 'N/A')
                        boundary = data.get('result', {}).get('place', {}).get('boundary', [])
                        longitude = boundary[0] if len(boundary) > 0 else 'N/A'
                        latitude = boundary[1] if len(boundary) > 1 else 'N/A'

                        store_data = {
                            'id': place_id,
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

                # 진행 상황
                save_progress(index)
                time.sleep(random.uniform(1, 3))

            except Exception as e:
                print(f"Error during crawling: {e}")
                save_progress(index)  # 예외 발생 시에도 진행 상황 저장
                if "503" in str(e):
                    raise  # 503 예외는 프로그램을 중단시키도록 설정
                continue  # 다른 예외는 계속 진행

    finally:
        cursor.close()
        conn.close()
