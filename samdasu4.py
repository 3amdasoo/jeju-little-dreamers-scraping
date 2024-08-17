import os
import requests
import pandas as pd
import time
import random
import re
from collections import deque
import csv

# 실패한 요소를 기록하는 파일 경로
failed_requests_file = './failed_requests.txt'
progress_file = './progress.txt'  # 크롤링 진행 상태를 저장하는 파일
output_file = './output_data.csv'  # 결과 데이터를 저장할 CSV 파일 경로

# 다양한 User-Agent 헤더 설정 (기존 것)
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Gecko/20100101 Firefox/92.0",
    "Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.152 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/92.0.902.67",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:91.0) Gecko/20100101 Firefox/91.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:93.0) Gecko/20100101 Firefox/93.0",
    "Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.164 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; AS; rv:11.0) like Gecko",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:90.0) Gecko/20100101 Firefox/90.0",
    "Mozilla/5.0 (iPad; CPU OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_5_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15",
    "Mozilla/5.0 (Linux; Android 10; SM-N960F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.101 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 13_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; SM-A505F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.72 Mobile Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
    "Mozilla/5.0 (iPad; CPU OS 14_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.1 Mobile/15E148 Safari/604.1"
]


# Sec-Ch-Ua 버전 정보도 랜덤화
sec_ch_ua = [
    '"Chromium";v="126", "Not A(Brand";v="8", "Google Chrome";v="126"',
    '"Chromium";v="122", "Not A(Brand";v="6", "Google Chrome";v="122"',
    '"Chromium";v="120", "Not A(Brand";v="6", "Google Chrome";v="120"'
]

# Sec-Ch-Ua-Platform 정보도 변경 가능하게 설정
sec_ch_ua_platforms = ['"Windows"', '"macOS"', '"Linux"', '"Android"']

# Accept-Language도 랜덤하게 추가
accept_languages = [
    "en-US,en;q=0.9",
    "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7"
]

def get_headers():
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": random.choice(accept_languages),  # 랜덤한 Accept-Language 선택
        "Sec-Ch-Ua": random.choice(sec_ch_ua),  # 랜덤한 Sec-Ch-Ua 선택
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": random.choice(sec_ch_ua_platforms),  # 랜덤한 Sec-Ch-Ua-Platform 선택
        "User-Agent": random.choice(user_agents),  # 랜덤한 User-Agent 선택
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    }
    return headers


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


# HTTP 요청 보내기 함수
def get_data(url):
    headers = get_headers()

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
            time.sleep(random.uniform(1, 3))
            return None
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None


# 결과 데이터를 CSV 파일에 저장하는 함수
def save_to_csv(data, file_path):
    file_exists = os.path.isfile(file_path)
    with open(file_path, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['id', 'name', 'address', 'tel', 'category', 'latitude', 'longitude',
                                               'menu_info'])

        # CSV 파일에 처음 쓸 때는 헤더 추가
        if not file_exists:
            writer.writeheader()

        writer.writerow(data)


# CSV 파일 읽기
df = pd.read_csv('제주특별자치도_아동급식카드가맹점현황_20240520.csv', encoding='cp949')

# 기본 URL
base_url = "https://map.naver.com/p/api/search/allSearch?type=all&searchCoord=126.29921620000067%3B33.352623099999775&query="

# 크롤링 시작점 불러오기
start_index = load_progress()

# df에서 시작 지점부터 끝까지 선택
df = df.iloc[start_index:]

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
                    category = ', '.join(item.get('category', [])) if isinstance(item.get('category', []),
                                                                                 list) else item.get('category', 'N/A')
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

                    # 데이터베이스 삽입 대신 CSV에 저장
                    save_to_csv(store_data, output_file)
                    print(f"Saved data to CSV for store: {name}")
                    print("-" * 50)

            # 진행 상황 저장
            save_progress(index)
            time.sleep(random.uniform(1, 3))

        except Exception as e:
            print(f"Error during crawling: {e}")
            save_progress(index)  # 예외 발생 시에도 진행 상황 저장
            if "503" in str(e):
                exit()  # 503 예외는 프로그램을 중단시키도록 설정
            continue  # 다른 예외는 계속 진행

finally:
    print("Crawling completed.")
