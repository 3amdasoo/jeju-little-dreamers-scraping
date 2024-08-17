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

# CSV 필드명 설정
fieldnames = ['id', 'name', 'address', 'tel', 'category', 'latitude', 'longitude', 'menu_info', 'isNotGoodForChild',
              'isFoodSelling']


# 중단 지점 기록 및 읽기 함수
def write_checkpoint(checkpoint_file, line_number):
    with open(checkpoint_file, 'w') as f:
        f.write(str(line_number))


def read_checkpoint(checkpoint_file):
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            return int(f.read().strip())
    return 0


# OpenAI 클라이언트 요청 함수
def process_row_with_gpt(row):
    # 각 행의 데이터를 조합하여 프롬프트 생성
    prompt = (f"장소 이름: {row['name']}, 카테고리: {row['category']}, 메뉴: {row.get('menu_info', '메뉴 정보 없음')}\n"
              f"이 장소는 음식점이거나 음식을 판매하는 곳입니까 (예: 음식점, 편의점, 카페, 슈퍼마켓, 농협 등)? 음식을 판매하면 True, 판매하지 않으면 False로만 답하세요.\n"
              f"또한 이 장소는 아동이나 청소년이 출입하면 안 되는 장소입니까? 아동 청소년이 출입하면 안 되는 장소면 True, 출입해도 되는 장소면 False로만 답하세요.\n"
              f"절대로 무슨 일이 있어도 너는 불린값 두 단어만 말해야 해.")

    # OpenAI API 호출 (chat completions 사용)
    print(f"[INFO] '{row['name']}'에 대해 OpenAI에 요청 중...")
    response = client.chat.completions.create(
        messages=[
            {"role": "system", "content": "너는 아동 및 청소년이 출입할 수 있는지, 그리고 음식점 또는 음식을 판매하는 장소 여부를 판단하는 전문가입니다."},
            {"role": "user", "content": prompt}
        ],
        model="gpt-4o-mini",
        max_tokens=100,
    )

    return response.choices[0].message.content.strip()  # 응답을 처리하는 부분


def parse_gpt_response(gpt_result):
    # GPT 응답을 소문자로 변환하여 쉼표로 분리
    responses = gpt_result.lower().replace(' ', '').split(',')

    print(f"[DEBUG] GPT 응답: {responses}")  # 응답을 출력하여 확인

    # 기본값 설정
    is_food_selling, is_restricted = None, None

    try:
        # 두 개의 응답 값이 있어야 함 (첫 번째는 음식점 여부, 두 번째는 출입 금지 여부)
        if len(responses) == 2:
            is_food_selling = responses[0].strip()  # 첫 번째 값 (음식점 여부)
            is_restricted = responses[1].strip()  # 두 번째 값 (아동 청소년 출입 금지 여부)

            # 응답이 "true" 또는 "false"만 포함하는지 확인
            if is_food_selling not in ['true', 'false'] or is_restricted not in ['true', 'false']:
                raise ValueError("Invalid True/False response")

            # "true"면 True, "false"면 False로 변환
            is_food_selling = is_food_selling == 'true'
            is_restricted = is_restricted == 'true'
        else:
            raise ValueError("Incomplete response from GPT")

    except Exception as e:
        print(f"[ERROR] 잘못된 응답: {e}")
        # 유효하지 않은 응답일 경우 None 반환
        return None, None

    return is_food_selling, is_restricted


# CSV 파일 처리 함수
def process_csv(input_file, output_clean_file, output_restricted_file, checkpoint_file):
    # 중단 지점 확인
    start_line = read_checkpoint(checkpoint_file)

    with open(input_file, mode='r', encoding='utf-8') as infile, \
            open(output_clean_file, mode='a', newline='', encoding='utf-8') as outfile_clean, \
            open(output_restricted_file, mode='a', newline='', encoding='utf-8') as outfile_restricted:

        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames + ['아동 청소년 출입 금지', '음식점 여부']

        # 중간 재시작인 경우, 이미 존재하는 헤더를 건너뛰기 위해 헤더 추가 여부 체크
        if start_line == 0:
            clean_writer = csv.DictWriter(outfile_clean, fieldnames=fieldnames)
            restricted_writer = csv.DictWriter(outfile_restricted, fieldnames=fieldnames)
            clean_writer.writeheader()
            restricted_writer.writeheader()

        else:
            clean_writer = csv.DictWriter(outfile_clean, fieldnames=fieldnames)
            restricted_writer = csv.DictWriter(outfile_restricted, fieldnames=fieldnames)

        # 파일을 다시 처음으로 되돌려서 시작 지점까지 건너뜀
        for _ in range(start_line):
            next(reader)

        print(f"[INFO] 총 {start_line} 지점에서부터 데이터를 처리합니다.")
        buffer_clean = []
        buffer_restricted = []

        for i, row in enumerate(reader, start=start_line + 1):
            # OpenAI API를 통해 장소가 음식점인지 및 출입 금지 여부 확인
            print(f"[INFO] {i} - '{row['name']}' 처리 중...")
            gpt_result = process_row_with_gpt(row)
            is_food_selling, is_restricted = parse_gpt_response(gpt_result)

            # 유효한 응답만 처리
            if is_food_selling is not None and is_restricted is not None:
                # 아동 청소년 출입 금지 및 음식점 여부를 행에 추가
                row['음식점 여부'] = is_food_selling
                row['아동 청소년 출입 금지'] = is_restricted

                # 1. 출입 금지 장소는 output_restricted에 저장
                if is_restricted:
                    print(f"[INFO] '{row['name']}'는 아동 청소년 출입 금지 장소입니다. (저장됨: output_restricted.csv)")
                    buffer_restricted.append(row)
                # 2. 나머지 장소는 output_clean에 저장
                else:
                    print(f"[INFO] '{row['name']}'는 안전한 장소입니다. (저장됨: output_clean.csv)")
                    buffer_clean.append(row)

            # 10번째마다 데이터를 CSV 파일에 기록 및 체크포인트 저장
            if i % 10 == 0:
                print("[INFO] 중간 저장: 10개 데이터를 CSV 파일에 저장합니다.")
                clean_writer.writerows(buffer_clean)
                restricted_writer.writerows(buffer_restricted)
                buffer_clean.clear()
                buffer_restricted.clear()
                write_checkpoint(checkpoint_file, i)

        # 남은 데이터를 파일에 기록
        if buffer_clean or buffer_restricted:
            print("[INFO] 마지막 데이터를 CSV 파일에 저장합니다.")
            clean_writer.writerows(buffer_clean)
            restricted_writer.writerows(buffer_restricted)

        # 최종 체크포인트 기록
        write_checkpoint(checkpoint_file, i)

        print("[INFO] 모든 데이터를 처리했습니다.")


# 입력 및 출력 파일 경로 설정
input_file = 'output_data.csv'
output_clean_file = 'output_clean.csv'
output_restricted_file = 'output_restricted.csv'
checkpoint_file = 'checkpoint.txt'

# CSV 파일 처리 실행
process_csv(input_file, output_clean_file, output_restricted_file, checkpoint_file)

print("[INFO] 처리가 완료되었습니다. 결과가 output_clean.csv 및 output_restricted.csv에 저장되었습니다.")
