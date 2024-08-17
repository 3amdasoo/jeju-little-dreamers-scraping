import csv
import os
import yaml
from openai import OpenAI


# YAML 파일에서 API 키를 불러오는 함수
def load_api_key_from_yaml(file_path):
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
        return config['openai']['openaiKey']


# OpenAI 클라이언트 초기화
api_key = load_api_key_from_yaml('properties.yml')  # YAML 파일 경로에 맞게 수정
client = OpenAI(api_key=api_key)


# OpenAI 클라이언트 요청 함수
def process_row_with_gpt(row):
    # 각 행의 데이터를 조합하여 프롬프트 생성
    prompt = (f"장소 이름: {row['name']}, 카테고리: {row['category']}, 메뉴: {row.get('menu_info', '메뉴 정보 없음')}\n"
              f"이 장소는 아동이나 청소년이 출입하면 안 되는 장소입니까? 'True' 또는 'False'로만 답하세요.\n"
              f"또한, 이 장소는 음식점입니까? 'True' 또는 'False'로만 답하세요.")

    # OpenAI API 호출 (chat completions 사용)
    print(f"[INFO] '{row['name']}'에 대해 OpenAI에 요청 중...")
    chat_completion = client.chat.completions.create(
        messages=[
            {"role": "system", "content": "너는 아동 및 청소년이 출입할 수 있는지, 그리고 음식점 여부를 판단하는 전문가입니다."},
            {"role": "user", "content": prompt}
        ],
        model="gpt-3.5-turbo",  # 또는 gpt-4, 필요에 따라 변경
        max_tokens=100,
    )

    return chat_completion.choices[0].message.content.strip()


# 아동 청소년 출입 금지 여부 및 음식점 여부 판단 함수
def parse_gpt_response(gpt_result):
    # GPT 응답에서 True/False만 추출
    responses = gpt_result.split('\n')
    is_restricted = False
    is_restaurant = False

    for response in responses:
        if '출입하면 안 되는 장소' in response:
            is_restricted = 'True' in response
        if '음식점' in response:
            is_restaurant = 'True' in response

    return is_restricted, is_restaurant


# CSV 파일 처리 함수
def process_csv(input_file, output_clean_file, output_restricted_file):
    with open(input_file, mode='r', encoding='utf-8') as infile, \
            open(output_clean_file, mode='w', newline='', encoding='utf-8') as outfile_clean, \
            open(output_restricted_file, mode='w', newline='', encoding='utf-8') as outfile_restricted:

        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames + ['아동 청소년 출입 금지', '음식점 여부']
        clean_writer = csv.DictWriter(outfile_clean, fieldnames=fieldnames)
        restricted_writer = csv.DictWriter(outfile_restricted, fieldnames=fieldnames)

        clean_writer.writeheader()
        restricted_writer.writeheader()

        total_rows = sum(1 for _ in reader)
        infile.seek(0)  # 파일을 다시 처음으로 되돌림
        reader = csv.DictReader(infile)

        print(f"[INFO] 총 {total_rows}개의 데이터를 처리합니다.")

        for i, row in enumerate(reader, 1):
            # OpenAI API를 통해 장소가 출입 금지인지 및 음식점 여부 확인
            print(f"[INFO] {i}/{total_rows} - '{row['name']}' 처리 중...")
            gpt_result = process_row_with_gpt(row)
            is_restricted, is_restaurant = parse_gpt_response(gpt_result)

            # 아동 청소년 출입 금지 및 음식점 여부를 행에 추가
            row['아동 청소년 출입 금지'] = is_restricted
            row['음식점 여부'] = is_restaurant

            # 1. 출입 금지 장소를 output_restricted에 저장
            if is_restricted:
                print(f"[INFO] '{row['name']}'는 아동 청소년 출입 금지 장소입니다. (저장됨: output_restricted.csv)")
                restricted_writer.writerow(row)
            # 2. 나머지 장소는 output_clean에 저장
            else:
                print(f"[INFO] '{row['name']}'는 안전한 장소입니다. (저장됨: output_clean.csv)")
                clean_writer.writerow(row)

        print("[INFO] 모든 데이터를 처리했습니다.")


# 입력 및 출력 파일 경로 설정
input_file = 'output_data.csv'
output_clean_file = 'output_clean.csv'
output_restricted_file = 'output_restricted.csv'

# CSV 파일 처리 실행
process_csv(input_file, output_clean_file, output_restricted_file)

print("[INFO] 처리가 완료되었습니다. 결과가 output_clean.csv 및 output_restricted.csv에 저장되었습니다.")
