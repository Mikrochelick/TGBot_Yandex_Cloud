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
    bucket_name = os.environ['BUCKET_NAME']
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
        print(f"Возникла ошибка при чтении файла из Yandex Object Storage: {key}")
        print(e)
        return None


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
        data_bytes = json.dumps(data)  # .encode('utf-8')
        bucket_name = os.environ['BUCKET_NAME']
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
        bucket_name = os.environ['BUCKET_NAME']
        # Получение списка объектов (файлов) в указанной директории
        response = yandex_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        # Список файлов
        file_list = [obj['Key'] for obj in response['Contents']]
        # Вернуть список файлов (в данном примере просто выводим его)
        # print(file_list)
        return file_list

    except Exception as e:
        print("Возникла ошибка при чтении списка в Yandex Object Storage:")
        print(e)
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }


def handler(object, context=None):
    try:
        # Проверка кодировки base64
        try:
            body = base64.b64decode(object.get('body')).decode('utf-8')
        except:
            pass
        # Обработка POST от стороннего приложения с API в запросе
        if object.get('queryStringParameters').get('APIKEY') != None and object.get('httpMethod') == 'POST':
            api_key = object.get('queryStringParameters').get('APIKEY')
            chat_id = read_file_from_yandex_s3(f'api_and_chatid/{api_key}.txt')
            if chat_id == None: return 'Wrong APIKEY'
            send_message(body, chat_id)
            return 'Ok'

        # Обработка GET от стороннего приложения с API в запросе
        if object.get('queryStringParameters').get('APIKEY') != None and object.get('httpMethod') == 'GET':
            api_key = object.get('queryStringParameters').get('APIKEY')
            chat_id = read_file_from_yandex_s3(f'api_and_chatid/{api_key}.txt')
            if chat_id == None: return 'Wrong APIKEY'
            oldest_file = list_oldest_file_in_folder(f'history_message/{chat_id}')
            body = read_file_from_yandex_s3(oldest_file['Key'])
            print('Текст ответа: ', body)
            delete_oldest_file_in_folder(oldest_file['Key'])
            return {
                "statusCode": 200,
                "body": body
            }

        body = json.loads(object['body'])
        # Другие события не связанные с отправкой каких либо сообщений
        if body.get('my_chat_member'):
            save_data_to_yandex_s3(object, f'mychatmember/{generate_apikey()}.txt')
            return 'Ok'

        if body.get('message'):
            user_name = body.get('message').get('from').get('username')
            chat_id = body.get('message').get('chat').get('id')
            text = body.get('message').get('text')
            # Проверяем, есть ли у данного пользователя API_KEY
            # Если его нет, выдаем ключ и сохраняем соотвествие в Object storage
            # Если есть, показываем его
            if text == '/start' and f'userwithapikey/{chat_id}.txt' in list_files_in_yandex_s3('userwithapikey'):
                user_api_key = read_file_from_yandex_s3(f'userwithapikey/{chat_id}.txt').get('user_api_key')
                #                save_data_to_yandex_s3(text, f'history_message/{chat_id}/{str(generate_apikey())}.txt')
                send_message(
                    f'''
У вас уже есть API ключ, вот он:
{user_api_key}
Ваше приложение может отправлять в этот чат текст: 
requests.post("{os.environ['URL']}?APIKEY={user_api_key}", data="text")
а также получать реплики пользователя (или пустую строку в ответе):
requests.get("{os.environ['URL']}?APIKEY={user_api_key}")
                    '''
                    , chat_id)
                return user_api_key

            if text == '/start' and (f'userwithapikey/{chat_id}.txt' not in list_files_in_yandex_s3('userwithapikey')):
                user_api_key = str(generate_apikey())
                object['user_api_key'] = f'{user_api_key}'
                save_data_to_yandex_s3(object,
                                       f'userwithapikey/{chat_id}.txt')  # Сохраняем в папку userwithapikey весь объект с ключом
                save_data_to_yandex_s3(chat_id,
                                       f'api_and_chatid/{user_api_key}.txt')  # Сохраняем в папку api_and_chatid файл с именем апи, внутри будет лежать chat_id
                #                save_data_to_yandex_s3(text, f'history_message/{chat_id}/{str(generate_apikey())}.txt}')
                send_message(f'''
Вам выдан API ключ, вот он:
{user_api_key}
Теперь ваше приложение может отправлять в этот чат текст: 
requests.post("{os.environ.get('URL')}?APIKEY={user_api_key}", data="text")
а также получать реплики пользователя (или пустую строку в ответе):
requests.get("{os.environ.get('URL')}?APIKEY={user_api_key}")
                    '''
                             , chat_id)
                return user_api_key

            else:
                #                send_message(f'Вы написали: {text}\nПока что я только умею выдавать или показывать уже выданные API ключи', chat_id)
                save_data_to_yandex_s3(text, f'history_message/{chat_id}/{str(generate_apikey())}.txt')
                return 'Ok'
        else:
            save_data_to_yandex_s3(object, f'другиесобытия/{generate_apikey()}.txt')
            return 'Ok'

    except Exception as e:
        print(repr(e))
        save_data_to_yandex_s3(object, f'мусорсошибками/{generate_apikey()}.txt')
        print('В Body нет JSON структуры')


def generate_apikey(): return str(uuid.uuid4())


def send_message(text, chat_id):
    url = f'https://api.telegram.org/bot%s/sendMessage' % os.environ['BOT_TOKEN']
    data = {'chat_id': chat_id, 'text': text}
    response = requests.post(url, data=data)
    return response


def list_oldest_file_in_folder(folder_name):
    bucket_name = os.environ['BUCKET_NAME']

    # Инициализируйте клиента S3
    yandex_client = boto3.client('s3',
                                 aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                                 aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                                 region_name='ru-central1',
                                 endpoint_url='https://storage.yandexcloud.net'
                                 )

    try:
        # Получение списка объектов (файлов) в папке
        response = yandex_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)

        # Поиск самого старого файла на основе времени последнего изменения (или времени создания)
        oldest_file = min(response['Contents'], key=lambda x: x['LastModified'])

        print(f"Файл '{oldest_file['Key']}' успешно удален из папки '{folder_name}' в хранилище '{bucket_name}'.")
        return oldest_file
    except Exception as e:
        print(f"Ошибка при удалении файла: {str(e)}")
        return False


def delete_oldest_file_in_folder(folder_name):
    bucket_name = os.environ['BUCKET_NAME']
    # Инициализируйте клиента S3
    yandex_client = boto3.client('s3',
                                 aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                                 aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                                 region_name='ru-central1',
                                 endpoint_url='https://storage.yandexcloud.net'
                                 )

    try:
        # Получение списка объектов (файлов) в папке
        response = yandex_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)

        # Поиск самого старого файла на основе времени последнего изменения (или времени создания)
        oldest_file = min(response['Contents'], key=lambda x: x['LastModified'])

        # Удаление самого старого файла
        yandex_client.delete_object(Bucket=bucket_name, Key=oldest_file['Key'])

        print(f"Файл '{oldest_file['Key']}' успешно удален из папки '{folder_name}' в хранилище '{bucket_name}'.")
        return True
    except Exception as e:
        print(f"Ошибка при удалении файла: {str(e)}")
        return False


if _name_ == '_main_':
    # Send Message
    handler(
        {'httpMethod': 'POST',
         'headers': {'Accept-Encoding': 'gzip, deflate', 'Content-Length': '275', 'Content-Type': 'application/json',
                     'Host': 'functions.yandexcloud.net',
                     'Uber-Trace-Id': 'ef83aac411f81701:3c9d7a6c2718f92d:ef83aac411f81701:1',
                     'X-Forwarded-For': '91.108.6.30', 'X-Real-Remote-Address': '[91.108.6.30]:48338',
                     'X-Request-Id': 'c8153877-096b-4eb5-834d-5c159c3093c1',
                     'X-Trace-Id': 'd7f8ade5-9443-48a4-a416-3c437e80b490'}, 'url': '', 'params': {},
         'multiValueParams': {}, 'pathParams': {},
         'multiValueHeaders': {'Accept-Encoding': ['gzip, deflate'], 'Content-Length': ['275'],
                               'Content-Type': ['application/json'], 'Host': ['functions.yandexcloud.net'],
                               'Uber-Trace-Id': ['ef83aac411f81701:3c9d7a6c2718f92d:ef83aac411f81701:1'],
                               'X-Forwarded-For': ['91.108.6.30'], 'X-Real-Remote-Address': ['[91.108.6.30]:48338'],
                               'X-Request-Id': ['c8153877-096b-4eb5-834d-5c159c3093c1'],
                               'X-Trace-Id': ['d7f8ade5-9443-48a4-a416-3c437e80b490']}, 'queryStringParameters': {},
         'multiValueQueryStringParameters': {},
         'requestContext': {'identity': {'sourceIp': '91.108.6.30', 'userAgent': ''}, 'httpMethod': 'POST',
                            'requestId': 'c8153877-096b-4eb5-834d-5c159c3093c1',
                            'requestTime': '30/Jul/2023:09:55:36 +0000', 'requestTimeEpoch': 1690710936},
         'body': '{"update_id":964750237,\n"message":{"message_id":24,"from":{"id":340942954,"is_bot":false,"first_name":"Andrey","username":"AndyCubic","language_code":"en"},"chat":{"id":340942954,"first_name":"Andrey","username":"AndyCubic","type":"private"},"date":1690710936,"text":"zzzz"}}',
         'isBase64Encoded': False}
    )

    # Get Message
    handler(
        {'httpMethod': 'GET',
         'headers': {'Accept': '/', 'Accept-Encoding': 'gzip, deflate', 'Host': 'functions.yandexcloud.net',
                     'Uber-Trace-Id': '10819faf8f626604:7a48d8d69ceca00f:10819faf8f626604:1',
                     'User-Agent': 'python-requests/2.28.1', 'X-Forwarded-For': '212.58.114.232',
                     'X-Real-Remote-Address': '[212.58.114.232]:22606',
                     'X-Request-Id': '59978a47-16df-4a46-854d-941e22a2e124',
                     'X-Trace-Id': '8be5c37a-f7b3-4ed8-9617-804d386d59eb'}, 'url': '', 'params': {},
         'multiValueParams': {}, 'pathParams': {},
         'multiValueHeaders': {'Accept': ['/'], 'Accept-Encoding': ['gzip, deflate'],
                               'Host': ['functions.yandexcloud.net'],
                               'Uber-Trace-Id': ['10819faf8f626604:7a48d8d69ceca00f:10819faf8f626604:1'],
                               'User-Agent': ['python-requests/2.28.1'], 'X-Forwarded-For': ['212.58.114.232'],
                               'X-Real-Remote-Address': ['[212.58.114.232]:22606'],
                               'X-Request-Id': ['59978a47-16df-4a46-854d-941e22a2e124'],
                               'X-Trace-Id': ['8be5c37a-f7b3-4ed8-9617-804d386d59eb']},
         'queryStringParameters': {'APIKEY': 'f314e9c7-1a4e-499c-903b-2d5f7d65b425'},
         'multiValueQueryStringParameters': {'APIKEY': ['f314e9c7-1a4e-499c-903b-2d5f7d65b425']},
         'requestContext': {'identity': {'sourceIp': '212.58.114.232', 'userAgent': 'python-requests/2.28.1'},
                            'httpMethod': 'GET', 'requestId': '59978a47-16df-4a46-854d-941e22a2e124',
                            'requestTime': '30/Jul/2023:09:57:40 +0000', 'requestTimeEpoch': 1690711060}, 'body': '',
         'isBase64Encoded': True}
    )