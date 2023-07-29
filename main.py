import json
import boto3
import uuid
import requests
import os
import base64


def read_file_from_yandex_s3(key):
    # Инициализируйте клиента S3
    yandex_client = boto3.client('s3',
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
        region_name='ru-central1',
        endpoint_url='https://storage.yandexcloud.net'
    )
    bucket_name = 'storageusersapi'
    try:
        # Чтение файла из S3-хранилища Yandex
        response = yandex_client.get_object(Bucket=bucket_name, Key=key)
        # Получение содержимого файла
        file_content = response['Body'].read()
        file_content = file_content.decode('utf-8')
        file_content = json.loads(file_content)
        # Вернуть содержимое файла
        return file_content
    except Exception as e:
        print("Возникла ошибка при чтении файла из Yandex Object Storage:")
        print(e)
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }


def save_data_to_yandex_s3(data, object_name):
    # Инициализируем клиент Yandex Object Storage
    yandex_client = boto3.client('s3',
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
        region_name='ru-central1',
        endpoint_url='https://storage.yandexcloud.net'
    )
    try:
        # Преобразуем данные в байтовый формат
        data_bytes = json.dumps(data) #.encode('utf-8')
        bucket_name = 'storageusersapi'
        # Загружаем данные в Yandex Object Storage
        yandex_client.put_object(Bucket=bucket_name, Key=object_name, Body=data_bytes)
        print(f"Данные успешно сохранены в Yandex Object Storage: s3://{bucket_name}/{object_name}")
    except Exception as e:
        print("Возникла ошибка при сохранении файла в Yandex Object Storage:")
        print(e)
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }


def list_files_in_yandex_s3(prefix):
    # Инициализируйте клиента S3
    yandex_client = boto3.client('s3',
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
        region_name='ru-central1',
        endpoint_url='https://storage.yandexcloud.net'
    )
    try:
        bucket_name = 'storageusersapi'
        # Получение списка объектов (файлов) в указанной директории
        response = yandex_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        # Список файлов
        file_list = [obj['Key'] for obj in response['Contents']]
        # Вернуть список файлов (в данном примере просто выводим его)
        #print(file_list)
        return file_list

    except Exception as e:
        print("Возникла ошибка при чтении списка в Yandex Object Storage:")
        print(e)
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }


def process_event(object):
    try:
        # Проверка кодировки base64
        if is_base64_encoded(object.get('body')):
            body = base64.b64decode(object.get('body')).decode('utf-8')

        body = json.loads(object['body'])
        if body.get('message'):

            user_name = body.get('message').get('from').get('username')
            chat_id = body.get('message').get('chat').get('id')
            text = body.get('message').get('text')
            # Проверяем, есть ли у данного пользователя API_KEY
            # Если его нет, выдаем ключ и сохраняем соотвествие в Object storage
            #spisok = list_files_in_yandex_s3('userwithapikey')
            # Если есть, показываем его
            if text == '/start' and f'userwithapikey/{chat_id}.txt' in list_files_in_yandex_s3('userwithapikey'):
                user_api_key = read_file_from_yandex_s3(f'userwithapikey/{chat_id}.txt').get('user_api_key')
                send_message(f'У вас уже есть API ключ, вот он:\n {user_api_key}', chat_id)
                return 0
            if text == '/start' and (f'userwithapikey/{chat_id}.txt' not in list_files_in_yandex_s3('userwithapikey')):
                user_api_key = generate_apikey()
                object['user_api_key'] = f'{user_api_key}'
                save_data_to_yandex_s3(object, f'userwithapikey/{chat_id}.txt')
                send_message(f'Вам присвоен API ключ:\n{user_api_key}', chat_id)
                return 0
            else:
                send_message(f'Вы написали: {text}\nПока что я только умею выдавать или показывать уже выданные API ключи', chat_id)
        # Другие события не связанные с отправкой каких либо сообщений
        if body.get('my_chat_member'):
            save_data_to_yandex_s3(object, f'mychatmember/{generate_apikey()}.txt')
        else:
            save_data_to_yandex_s3(object, f'другиесобытия/{generate_apikey()}.txt')
    except:
        save_data_to_yandex_s3(object, f'мусорсошибками/{generate_apikey()}.txt')
        print('В Body нет JSON структуры')


def generate_apikey(): return uuid.uuid4()


def send_message(text, chat_id):
    url = f'https://api.telegram.org/bot%s/sendMessage' % os.environ['BOT_TOKEN']
    data = {'chat_id': chat_id, 'text': text}
    response = requests.post(url, data=data)
    return response


def is_base64_encoded(s):
    try:
        decoded_bytes = base64.b64decode(s)
        decoded_string = decoded_bytes.decode('utf-8')
        # Если успешно декодировалась без ошибок, значит, это base64
        return True
    except:
        # Если произошла ошибка при декодировании, значит, это не base64
        return False


def handler(event, context):
    # print(event)
    process_event(event)
    # return {
    #     'statusCode': 200,
    #     'body': 'Function is GOOD',
    # }


# Задайте необходимые параметры'
stroka = {'httpMethod': 'POST', 'headers': {'Accept-Encoding': 'gzip, deflate', 'Content-Length': '587', 'Content-Type': 'application/json', 'Host': 'functions.yandexcloud.net', 'Uber-Trace-Id': 'e376f4d1d53914fb:c1e2918a99d04464:e376f4d1d53914fb:1', 'X-Forwarded-For': '91.108.6.54', 'X-Real-Remote-Address': '[91.108.6.54]:43724', 'X-Request-Id': 'c333fb89-9d83-4caa-a5ec-ef0f3f2b46af', 'X-Trace-Id': '4f80ea0a-f740-4111-84dd-37b93bf49c3f'}, 'url': '', 'params': {}, 'multiValueParams': {}, 'pathParams': {}, 'multiValueHeaders': {'Accept-Encoding': ['gzip, deflate'], 'Content-Length': ['587'], 'Content-Type': ['application/json'], 'Host': ['functions.yandexcloud.net'], 'Uber-Trace-Id': ['e376f4d1d53914fb:c1e2918a99d04464:e376f4d1d53914fb:1'], 'X-Forwarded-For': ['91.108.6.54'], 'X-Real-Remote-Address': ['[91.108.6.54]:43724'], 'X-Request-Id': ['c333fb89-9d83-4caa-a5ec-ef0f3f2b46af'], 'X-Trace-Id': ['4f80ea0a-f740-4111-84dd-37b93bf49c3f']}, 'queryStringParameters': {}, 'multiValueQueryStringParameters': {}, 'requestContext': {'identity': {'sourceIp': '91.108.6.54', 'userAgent': ''}, 'httpMethod': 'POST', 'requestId': 'c333fb89-9d83-4caa-a5ec-ef0f3f2b46af', 'requestTime': '25/Jul/2023:09:19:44 +0000', 'requestTimeEpoch': 1690276784}, 'body': '{"update_id":970069515,\n"my_chat_member":{"chat":{"id":1293541810,"first_name":"\\u0414\\u0435\\u043d\\u0438\\u0441","username":"KucherDenz","type":"private"},"from":{"id":1293541810,"is_bot":false,"first_name":"\\u0414\\u0435\\u043d\\u0438\\u0441","username":"KucherDenz","language_code":"ru"},"date":1690272207,"old_chat_member":{"user":{"id":6327342146,"is_bot":true,"first_name":"yaproger","username":"yaprogerTelegrambot"},"status":"member"},"new_chat_member":{"user":{"id":6327342146,"is_bot":true,"first_name":"yaproger","username":"yaprogerTelegrambot"},"status":"kicked","until_date":0}}}', 'isBase64Encoded': False}
stroka1 = {'httpMethod': 'POST', 'headers': {'Accept-Encoding': 'gzip, deflate', 'Content-Length': '324', 'Content-Type': 'application/json', 'Host': 'functions.yandexcloud.net', 'Uber-Trace-Id': '7d167107210e5326:a51df0d1043266c5:7d167107210e5326:1', 'X-Forwarded-For': '91.108.6.54', 'X-Real-Remote-Address': '[91.108.6.54]:45268', 'X-Request-Id': 'b4a04eed-fd2e-4486-9c82-7e1e33747221', 'X-Trace-Id': 'a4d35ac8-120b-4c10-acf9-246a3ddabd6a'}, 'url': '', 'params': {}, 'multiValueParams': {}, 'pathParams': {}, 'multiValueHeaders': {'Accept-Encoding': ['gzip, deflate'], 'Content-Length': ['324'], 'Content-Type': ['application/json'], 'Host': ['functions.yandexcloud.net'], 'Uber-Trace-Id': ['7d167107210e5326:a51df0d1043266c5:7d167107210e5326:1'], 'X-Forwarded-For': ['91.108.6.54'], 'X-Real-Remote-Address': ['[91.108.6.54]:45268'], 'X-Request-Id': ['b4a04eed-fd2e-4486-9c82-7e1e33747221'], 'X-Trace-Id': ['a4d35ac8-120b-4c10-acf9-246a3ddabd6a']}, 'queryStringParameters': {}, 'multiValueQueryStringParameters': {}, 'requestContext': {'identity': {'sourceIp': '91.108.6.54', 'userAgent': ''}, 'httpMethod': 'POST', 'requestId': 'b4a04eed-fd2e-4486-9c82-7e1e33747221', 'requestTime': '25/Jul/2023:09:04:50 +0000', 'requestTimeEpoch': 1690275890}, 'body': '{"update_id":970069530,\n"message":{"message_id":186,"from":{"id":611910223,"is_bot":false,"first_name":"Yan","username":"vash_helper","language_code":"ru"},"chat":{"id":611910223,"first_name":"Yan","username":"vash_helper","type":"private"},"date":1690275890,"text":"\\u0440\\u0440\\u043e\\u043c\\u043e\\u0440\\u043c\\u043e\\u0440"}}', 'isBase64Encoded': False}
stroka2 = {'httpMethod': 'POST', 'headers': {'Accept-Encoding': 'gzip, deflate', 'Content-Length': '334', 'Content-Type': 'application/json', 'Host': 'functions.yandexcloud.net', 'Uber-Trace-Id': '1a6730fa27f7feb8:3d1980a296bc89bf:1a6730fa27f7feb8:1', 'X-Forwarded-For': '91.108.6.54', 'X-Real-Remote-Address': '[91.108.6.54]:47402', 'X-Request-Id': '68757cdc-aad3-4b45-b68e-944f6d01ec7e', 'X-Trace-Id': 'c80d5efb-ad32-4a70-a493-d90322410252'}, 'url': '', 'params': {}, 'multiValueParams': {}, 'pathParams': {}, 'multiValueHeaders': {'Accept-Encoding': ['gzip, deflate'], 'Content-Length': ['334'], 'Content-Type': ['application/json'], 'Host': ['functions.yandexcloud.net'], 'Uber-Trace-Id': ['1a6730fa27f7feb8:3d1980a296bc89bf:1a6730fa27f7feb8:1'], 'X-Forwarded-For': ['91.108.6.54'], 'X-Real-Remote-Address': ['[91.108.6.54]:47402'], 'X-Request-Id': ['68757cdc-aad3-4b45-b68e-944f6d01ec7e'], 'X-Trace-Id': ['c80d5efb-ad32-4a70-a493-d90322410252']}, 'queryStringParameters': {}, 'multiValueQueryStringParameters': {}, 'requestContext': {'identity': {'sourceIp': '91.108.6.54', 'userAgent': ''}, 'httpMethod': 'POST', 'requestId': '68757cdc-aad3-4b45-b68e-944f6d01ec7e', 'requestTime': '27/Jul/2023:09:56:56 +0000', 'requestTimeEpoch': 1690451816}, 'body': '{"update_id":970069533,\n"message":{"message_id":192,"from":{"id":611910223,"is_bot":false,"first_name":"Yan","username":"vash_helper","language_code":"ru"},"chat":{"id":611910223,"first_name":"Yan","username":"vash_helper","type":"private"},"date":1690451816,"text":"/start","entities":[{"offset":0,"length":6,"type":"bot_command"}]}}', 'isBase64Encoded': False}
handler(stroka2, 0)

stroka3 = 'new btanch Yan'







