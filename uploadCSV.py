import csv
import pymysql


# MySQL 연결 설정
def connect_to_db():
    return pymysql.connect(
        user = 'junpark',
        password= 'Dnjswns3428!',
        host = 'junfirstdbinstance.che2useq0r4t.ap-northeast-2.rds.amazonaws.com',
        database = 'samdasu',
        charset = 'utf8mb4'  # 이모지 등 UTF-8을 처리하기 위해 utf8mb4 사용
    )

# 메뉴 데이터를 처리하여 테이블에 삽입하는 함수
def insert_menu_data(cursor, store_id, menu_data):
    # 메뉴 데이터 전처리 (메뉴1 가격 | 메뉴2 가격 형식)
    if menu_data:
        menu_items = menu_data.split('|')
        for item in menu_items:
            item_parts = item.strip().rsplit(' ', 1)  # 마지막 공백을 기준으로 나누기
            if len(item_parts) == 2:  # 메뉴와 가격이 한 쌍으로 있는 경우
                menu_name = item_parts[0]
                menu_price = item_parts[1]
                cursor.execute("""
                    INSERT INTO menu (id, store_id, name, price)
                    VALUES (UUID(), %s, %s, %s)
                """, (store_id, menu_name, menu_price))
                print("insert menu : " + item)


# CSV 데이터를 테이블에 삽입하는 함수
def insert_store_data_from_csv(csv_file):
    connection = connect_to_db()
    cursor = connection.cursor()

    try:
        with open(csv_file, 'r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)

            for row in csv_reader:
                # store 테이블에 데이터 삽입
                cursor.execute("""
                    INSERT INTO store (id, name, address, phone, category, latitude, longitude, image, isNotGoodForChild, isFoodSelling)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                row['id'], row['name'], row['address'], row['tel'], row['category'], row['latitude'], row['longitude'],
                None, row['isNotGoodForChild'], row['isFoodSelling']))

                store_id = row['id']  # 외래키로 사용될 store_id

                print(row)

                # 메뉴 정보가 있는 경우 처리
                menu_info = row.get('menu_info')
                if menu_info:  # 메뉴 형식이 있는 경우만 처리
                    insert_menu_data(cursor, store_id, menu_info)

            connection.commit()  # 커밋하여 변경사항 반영
    except Exception as e:
        connection.rollback()  # 에러 발생 시 롤백
        print(f"Error: {e}")
    finally:
        cursor.close()
        connection.close()


# CSV 파일 경로
csv_file = 'output_clean.csv'

# CSV 파일을 읽어 데이터베이스에 삽입
insert_store_data_from_csv(csv_file)
