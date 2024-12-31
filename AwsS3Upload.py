"""
このプログラムは、Azure BlobストレージからダウンロードされたファイルをAmazon S3にアップロードするためのものです。
以下の手順で動作します：
1. ローカルディレクトリからファイルを再帰的に探索します。
2. 各ファイルに対応するメタデータファイルを探し、Content-Typeを読み取ります。
3. Content-Typeが存在する場合、そのファイルをAmazon S3にアップロードします。
4. アップロードの成功または失敗をログに記録します。
"""

import boto3
import os
import logging
import configparser

# 設定ファイルの読み込み
config = configparser.ConfigParser()
config.read('config.ini')

# AWS S3の設定
s3_bucket_name = config['AWS']['S3BucketName']
aws_access_key_id = config['AWS']['AccessKeyId']
aws_secret_access_key = config['AWS']['SecretAccessKey']
aws_region = config['AWS']['Region']

# ログ設定
log_file_path = f'.\\{s3_bucket_name}_upload_log.txt'
logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region
    )
    logging.info('S3クライアントの初期化に成功しました。')
except Exception as e:
    logging.error(f'S3クライアントの初期化中にエラーが発生しました: {e}')
    exit(1)

# ダウンロードされたファイルのディレクトリ
local_path = f'.\\{s3_bucket_name}'

# リトライ用のファイル
retry_file_path = f'.\\{s3_bucket_name}_upload_retry.txt'

# リトライ用のファイルリストを読み込む
retry_files = set()
retry_mode = False
if os.path.exists(retry_file_path):
    with open(retry_file_path, 'r') as retry_file:
        retry_files = set(line.strip() for line in retry_file)
    retry_mode = True
else:
    open(retry_file_path, 'w').close()

processed_files = 0

for root, dirs, files in os.walk(local_path):
    for file in files:
        if file.endswith('.metadata'):
            continue
        
        file_path = os.path.join(root, file)
        metadata_file_path = file_path + '.metadata'
        
        if retry_mode and file_path not in retry_files:
            continue
        
        # メタデータファイルが存在するか確認
        if not os.path.exists(metadata_file_path):
            logging.warning(f'メタデータファイルが見つかりません: {metadata_file_path}')
            continue
        
        # メタデータファイルからContent-Typeを読み取る
        with open(metadata_file_path, 'r') as metadata_file:
            content_type = None
            for line in metadata_file:
                if line.startswith('Content-Type:'):
                    content_type = line.split(':', 1)[1].strip()
                    break
        
        if not content_type:
            logging.warning(f'Content-Typeが見つかりません: {metadata_file_path}')
            continue
        
        # S3にアップロード
        s3_key = os.path.relpath(file_path, local_path).replace('\\', '/')
        if s3_key.startswith('$root/'):
            s3_key = s3_key[len('$root/'):]
        
        try:
            if content_type:
                s3_client.upload_file(file_path, s3_bucket_name, s3_key, ExtraArgs={'ContentType': content_type})
            else:
                s3_client.upload_file(file_path, s3_bucket_name, s3_key)

            if file_path in retry_files:
                retry_files.remove(file_path)
            logging.info(f'ファイルをアップロードしました: {file_path} -> s3://{s3_bucket_name}/{s3_key}')

        except Exception as e:
            logging.error(f'ファイルのアップロード中にエラーが発生しました: {file_path} -> s3://{s3_bucket_name}/{s3_key}, エラー: {e}')
            retry_file.add(file_path)
            continue
        
        processed_files += 1
        if processed_files % 100 == 0:
            print(f'{processed_files}...', end='', flush=True)

# リトライ用のファイルを更新
if retry_files:
    with open(retry_file_path, 'w') as retry_file:
        for file_path in retry_files:
            retry_file.write(f'{file_path}\n')
    logging.info('一部のファイルのアップロードに失敗しました。リトライ用のファイルを更新しました。')
    print(f'一部のファイルのアップロードに失敗しました。リトライ用のファイルを更新しました。詳細はログファイル「{log_file_path}」を確認してください。')
else:
    if os.path.exists(retry_file_path):
        os.remove(retry_file_path)
    logging.info('全てのファイルのアップロードに成功しました。')
    print(f'全てのファイルのアップロードに成功しました。詳細はログファイル「{log_file_path}」を確認してください。')
