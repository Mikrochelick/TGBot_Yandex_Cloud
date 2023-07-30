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
        aws_access_key_id='os.environ['AWS_ACCESS_KEY_ID'],
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
        # Обработка POST от стороннего приложения с API в запросе
        if object.get('queryStringParameters').get('APIKEY') != None and object.get('httpMethod') == 'POST':
            api_key = object.get('queryStringParameters').get('APIKEY')
            chat_id = read_file_from_yandex_s3(f'api_and_chatid/{api_key}.txt')
            send_message(body, chat_id)
            return None
        # Обработка GET от стороннего приложения с API в запросе
        if object.get('queryStringParameters').get('APIKEY') != None and object.get('httpMethod') == 'GET':
            api_key = object.get('queryStringParameters').get('APIKEY')
            chat_id = read_file_from_yandex_s3(f'api_and_chatid/{api_key}.txt')
            body = read_file_from_yandex_s3(list_oldest_file_in_folder(f'history_massage/{chat_id}'))
            send_message(body, chat_id)
            delete_oldest_file_in_folder(f'history_message/{chat_id}')
            return None
        body = json.loads(object['body'])
        # Другие события не связанные с отправкой каких либо сообщений
        if body.get('my_chat_member'):
            save_data_to_yandex_s3(object, f'mychatmember/{generate_apikey()}.txt')

        if body.get('message'):

            user_name = body.get('message').get('from').get('username')
            chat_id = body.get('message').get('chat').get('id')
            text = body.get('message').get('text')
            # Проверяем, есть ли у данного пользователя API_KEY
            # Если его нет, выдаем ключ и сохраняем соотвествие в Object storage
            # Если есть, показываем его
            if text == '/start' and f'userwithapikey/{chat_id}.txt' in list_files_in_yandex_s3('userwithapikey'):
                user_api_key = read_file_from_yandex_s3(f'userwithapikey/{chat_id}.txt').get('user_api_key')
                save_data_to_yandex_s3(text, f'history_message/{chat_id}/{str(generate_apikey()).txt}')
                return send_message(
                    f'''
                    У вас уже есть API ключ, вот он:
                    {user_api_key}
                    Ваше приложение может отправлять в этот чат текст: 
                    requests.post(url+"?APIKEY=ghsdjkfghdksfgkjdf", data="text")
                    а также получать реплики пользователя (или пустую строку в ответе):
                    requests.get(url+"?APIKEY=ghsdjkfghdksfgkjdf")
                    '''
                    , chat_id)

            if text == '/start' and (f'userwithapikey/{chat_id}.txt' not in list_files_in_yandex_s3('userwithapikey')):
                user_api_key = str(generate_apikey())
                object['user_api_key'] = f'{user_api_key}'
                save_data_to_yandex_s3(object, f'userwithapikey/{chat_id}.txt') # Сохраняем в папку userwithapikey весь объект с ключом
                save_data_to_yandex_s3(chat_id, f'api_and_chatid/{user_api_key}.txt') #Сохраняем в папку api_and_chatid файл с именем апи, внутри будет лежать chat_id
                save_data_to_yandex_s3(text, f'history_message/{chat_id}/{str(generate_apikey()).txt}')
                return send_message(f'''
                    Вам выдан API ключ, вот он:
                    {user_api_key}
                    Теперь ваше приложение может отправлять в этот чат текст: 
                    requests.post(url+"?APIKEY=вашAPIключ", data="text")
                    а также получать реплики пользователя (или пустую строку в ответе):
                    requests.get(url+"?APIKEY=APIKEY=вашAPIключ")
                    '''
                    , chat_id)

            else:
                send_message(f'Вы написали: {text}\nПока что я только умею выдавать или показывать уже выданные API ключи', chat_id)
                save_data_to_yandex_s3(text, f'history_message/{chat_id}/{str(generate_apikey()).txt}')
        else:
            save_data_to_yandex_s3(object, f'другиесобытия/{generate_apikey()}.txt')

    except:
        save_data_to_yandex_s3(object, f'мусорсошибками/{generate_apikey()}.txt')
        print('В Body нет JSON структуры')


def generate_apikey(): return str(uuid.uuid4())


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


def handler(event, context):
    # print(event)
    process_event(event)
    # return {
    #     'statusCode': 200,
    #     'body': 'Function is GOOD',
    # }








