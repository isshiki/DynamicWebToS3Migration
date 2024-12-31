"""
このプログラムは、CMS管理のWebサイトのHTMLページを全てローカルにダウンロードするためのものです。
以下の手順で動作します：
1. サイトマップのURLを読み込み、データとして保持します。
2. トップページのHTMLをダウンロードし、ローカルディレクトリに保存します。
3. トップページ内のリンクと画像ソースを収集し、再帰的にダウンロードします。
4. 各HTMLファイルとリソースファイルをローカルディレクトリに保存し、メタデータファイルを作成します。
5. サイトマップのURLも同様に処理し、全てのページをダウンロードします。
6. Google検索用のサイトマップを作成し、ローカルに保存します。
7. 処理済みのURLとスキップしたURLをログファイルに書き出します。
"""

import os
import logging
import configparser
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from xml.dom.minidom import parseString

# 設定ファイルの読み込み
config = configparser.ConfigParser()
config.read('config.ini')

# トップページのURL
domain_name = config['Web']['domainName']
top_page_url = f'https://{domain_name}'

# ダウンロード先のローカルディレクトリ
top_local_name = config['Web']['topLocalName']
top_local_dir = f'.\\{top_local_name}'

# サイトマップのURL
sitemap_url_path = config['Web']['sitemapUrlPath']
# sitemap_url_name = os.path.basename(sitemap_url_path)
sitemap_url = f'{top_page_url}{sitemap_url_path}'
sitemap_file_path = f'{top_local_dir}{sitemap_url_path}'.replace('/', '\\')

# インデックスドキュメント名（デフォルトのルートオブジェクト）
index_file_name = config['Web']['indexFileName']
top_local_file = f'.\\{top_local_name}\\{index_file_name}'

# ログ設定
log_file_path = f'.\\{top_local_name}_download_log.txt'
logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
processed_file_path = f'.\\{top_local_name}_processed_log.txt'
skipped_file_path = f'.\\{top_local_name}_skipped_log.txt'
retry_file_path = f'.\\{top_local_name}_download_retry.txt'

# サイトマップを読み込む関数
def load_sitemap(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        sitemap = []
        
        # 名前空間を定義
        namespaces = {'ns': 'http://www.google.com/schemas/sitemap/0.84'}
        # ルート要素を解析
        root = ET.fromstring(response.content)
        
        # 各 <url> 要素を処理
        for url in root.findall('ns:url', namespaces):
            loc = url.find('ns:loc', namespaces).text

            lastmod_text = url.find('ns:lastmod', namespaces).text
            lastmod = datetime.strptime(lastmod_text, '%Y-%m-%dT%H:%M:%S%z') if lastmod_text else None
            
            changefreq = url.find('ns:changefreq', namespaces)
            changefreq = 'never'  # changefreq.text if changefreq is not None else 'never'
            
            priority = url.find('ns:priority', namespaces)
            priority = 0.5  # float(priority.text) if priority is not None else 0.5
            
            sitemap.append({
                'loc': loc,
                'lastmod': lastmod,
                'changefreq': changefreq,
                'priority': priority
            })
        
        # URL をアルファベット順にソート
        sitemap.sort(key=lambda x: x['loc'])
        return sitemap
    except requests.exceptions.RequestException as e:
        logging.error(f"サイトマップの読み込み中にエラーが発生しました: {e}")
        return []
    except ET.ParseError as e:
        logging.error(f"サイトマップの解析中にエラーが発生しました: {e}")
        return []
    except Exception as e:
        logging.error(f"予期しないエラーが発生しました: {e}")
        return []

# サイトマップのURLを読み込み、データとして保持
sitemap_data = load_sitemap(sitemap_url)

if not sitemap_data:
    print("サイトマップの読み込みに失敗しました。プログラムを終了します。")
    exit(1)

# 処理済みURLとスキップ済みURLを保持するセット
processed_urls = set()
skipped_urls = set()
retry_urls = set()

# リトライ用のURLリストを読み込む
retry_urls = set()
retry_mode = False
if os.path.exists(retry_file_path):
    with open(retry_file_path, 'r') as retry_file:
        retry_urls = set(line.strip() for line in retry_file)
    retry_mode = True
else:
    open(retry_file_path, 'w').close()

# Webアクセスの再試行回数
WEB_RETRY_COUNT = 3

# HTMLをダウンロードしてローカルに保存する関数
def download_html(url, local_path, retry_mode=False, recursive=True):
    if retry_mode and url not in retry_urls:
        return set()

    for attempt in range(WEB_RETRY_COUNT):
        try:
            response = requests.get(url)
            response.raise_for_status()
            
            # リダイレクトを判別
            if response.history:
                logging.info(f"URL '{url}' はリダイレクトされました。スキップします。")
                if url not in skipped_urls:
                    skipped_urls.add(url)
                return None

            # HTMLをパース
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 特定のHTMLブロックをカット
            for widget in soup.find_all('div', class_='widget'):
                if widget.find('h3') and widget.find('h3').text == '人気記事ランキング':
                    widget.decompose()
            view_sp = soup.find('li', id='view_sp')
            if view_sp:
                view_sp.decompose()
            
            # 特定のHTMLブロックを置き換え
            header_banner = soup.find('div', id='headerBanner')
            if header_banner:
                new_banner = BeautifulSoup('<div id="headerBanner"><span class="da-fg da-gray"><span class="da-bg da-yellow">　　本ページはアーカイブです。　　</span></span></div>\n', 'html.parser')
                header_banner.replace_with(new_banner)
                # new_banner.insert_after('\n')
            else:
                body_grid = soup.find('div', id='bodyGrid')
                if body_grid:
                    new_banner = BeautifulSoup('<div id="headerBanner"><span class="da-fg da-gray"><span class="da-bg da-yellow">　　本ページはアーカイブです。　　</span></span></div>\n', 'html.parser')
                    body_grid.insert_before(new_banner)
                    # body_grid.insert_before('\n')
            
            # ローカルディレクトリを作成
            local_dir = os.path.dirname(local_path)
            os.makedirs(local_dir, exist_ok=True)

            # response.contentの内容を書き換え
            content = str(soup).encode('utf-8').replace(b'/tagcloud?tag=', b'/tagcloud/')
            
            # HTMLファイルをローカルに保存
            with open(local_path, 'wb') as file:
                file.write(content)
            
            # MIMEタイプを取得してメタデータファイルに保存
            content_type = response.headers.get('Content-Type', 'text/html')
            with open(local_path + '.metadata', 'w') as metadata_file:
                metadata_file.write(f'Content-Type: {content_type}\n')
            
            if url in retry_urls:
                retry_urls.remove(url)
            logging.info(f"HTMLファイル '{url}' をダウンロードし、メタデータを保存しました。")
            
            # リンクと画像ソースを収集
            links = set()
            if recursive:
                for tag in soup.find_all(['a', 'img']):
                    href = tag.get('href')
                    src = tag.get('src')
                    if href:
                        if href.startswith(top_page_url) or not href.startswith("http"):
                            full_url = urljoin(url, href)
                            full_url = full_url.split('#')[0]  # ページ内リンクをカット
                            full_url = full_url.rstrip('/')  # 末尾のスラッシュをカット
                            if full_url.startswith(top_page_url):
                                links.add(full_url)
                    if src:
                        if src.startswith(top_page_url) or not src.startswith("http"):
                            full_url = urljoin(url, src)
                            if full_url.startswith(top_page_url):
                                links.add(full_url)
            return links
        except (requests.exceptions.RequestException, IOError) as e:
            logging.error(f"URL '{url}' のダウンロード中にエラーが発生しました (試行 {attempt + 1}/{WEB_RETRY_COUNT}): {e}")
            if attempt == WEB_RETRY_COUNT - 1:
                retry_urls.add(url)
                continue

# トップローカルディレクトリを作成
os.makedirs(top_local_dir, exist_ok=True)

# トップページのHTMLをダウンロード
page_links = download_html(top_page_url, top_local_file, retry_mode)
if page_links is None:
    page_links = set()
else:
    processed_urls.add(top_page_url)

# 収集したリンクを処理
progress_items = 1
while page_links:
    url = page_links.pop()
    
    # クエリパラメーターをカット
    is_tagcloud_param = url.startswith(f'{top_page_url}/tagcloud?tag=')
    if '?' in url and not is_tagcloud_param:
        original_url = url
        url = url.split('?')[0]
        if original_url not in skipped_urls:
            skipped_urls.add(original_url)

    if url in processed_urls:
        continue

    local_path = url.replace(top_page_url, top_local_dir).replace('/', '\\')
    if is_tagcloud_param:
        local_path = local_path.replace('\\tagcloud?tag=', '\\tagcloud\\')
    ext = os.path.splitext(local_path)[1]
    
    if ext and not is_tagcloud_param:  # 拡張子がある場合
        for attempt in range(WEB_RETRY_COUNT):
            try:
                response = requests.get(url)
                response.raise_for_status()
                
                # ローカルディレクトリを作成
                local_dir = os.path.dirname(local_path)
                os.makedirs(local_dir, exist_ok=True)

                # ファイルをローカルに保存
                with open(local_path, 'wb') as file:
                    file.write(response.content)
                
                # MIMEタイプを取得してメタデータファイルに保存
                content_type = response.headers.get('Content-Type', 'application/octet-stream')
                with open(local_path + '.metadata', 'w') as metadata_file:
                    metadata_file.write(f'Content-Type: {content_type}\n')
                
                if url in retry_urls:
                    retry_urls.remove(url)
                logging.info(f"{content_type} ファイル '{url}' をダウンロードし、メタデータを保存しました。")
                processed_urls.add(url)
                break
            except (requests.exceptions.RequestException, IOError) as e:
                logging.error(f"URL '{url}' のダウンロード中にエラーが発生しました (試行 {attempt + 1}/{WEB_RETRY_COUNT}): {e}")
                if attempt == WEB_RETRY_COUNT - 1:
                    retry_urls.add(url)
                    continue
    
    else:
        local_path = os.path.join(local_path, index_file_name)
    
    try:
        links = download_html(url, local_path, retry_mode)
        if links is None:
            links = set()
        else:
            processed_urls.add(url)
        page_links.update(links)
    except (requests.exceptions.RequestException, IOError) as e:
        logging.error(f"URL '{url}' のダウンロード中にエラーが発生しました: {e}")
        retry_urls.add(url)
    
    progress_items += 1
    if progress_items % 100 == 0:
        print(f'{progress_items}...', end='', flush=True)

# サイトマップのURLも同様に処理
progress_items = 0
for entry in sitemap_data:
    url = entry['loc']
    if url in processed_urls:
        continue
    
    local_path = url.replace(top_page_url, top_local_dir).replace('/', '\\')
    if not os.path.splitext(local_path)[1]:
        local_path = os.path.join(local_path, index_file_name)
    
    try:
        links = download_html(url, local_path, retry_mode, recursive=False)
        if links is None:
            links = set()
        else:
            processed_urls.add(url)        
        # page_links.update(links)  # サイトマップのリンクは再帰的に処理しない
    except requests.exceptions.RequestException as e:
        logging.error(f"URL '{url}' のダウンロード中にエラーが発生しました: {e}")
        retry_urls.add(url)
    
    progress_items += 1
    if progress_items % 100 == 0:
        print(f'{progress_items}...', end='', flush=True)

# Google検索用のサイトマップを作成
current_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S%z')
sitemap_entries = []
for url in sorted(processed_urls):
    priority = 0.8 if url == top_page_url else 0.5
    lastmod = next((entry['lastmod'].strftime('%Y-%m-%dT%H:%M:%S%z') for entry in sitemap_data if entry['loc'] == url), current_time)
    sitemap_entries.append(f"<url><loc>{url.replace('/tagcloud?tag=', '/tagcloud/')}</loc><lastmod>{lastmod}</lastmod><changefreq>never</changefreq><priority>{priority}</priority></url>")

# ローカルディレクトリを作成
sitemap_dir = os.path.dirname(sitemap_file_path)
os.makedirs(sitemap_dir, exist_ok=True)

# サイトマップを生成して見やすく整形して書き出す
sitemap_content = f'<?xml version="1.0" ?><urlset xmlns="http://www.google.com/schemas/sitemap/0.84">{"".join(sitemap_entries)}</urlset>'
dom = parseString(sitemap_content)
formatted_sitemap_content = dom.toprettyxml(indent="  ").replace('<?xml version="1.0" ?>\n', '')  # インデント幅を2スペースに設定
with open(sitemap_file_path, 'w', encoding='utf-8') as file:
    file.write(formatted_sitemap_content)

# サイトマップのメタデータを保存
with open(sitemap_file_path + '.metadata', 'w') as metadata_file:
    metadata_file.write('Content-Type: application/xml\n')

logging.info(f"サイトマップ '{sitemap_file_path}' を生成し、メタデータを保存しました。")
processed_urls.add(sitemap_url)

# 処理済みのURLをログファイルに書き出し
with open(processed_file_path, 'w') as file:
    for url in sorted(processed_urls):
        file.write(f'{url}\n')
logging.info('処理済みのURLを「{processed_file_path}」に記録しました。')

# スキップしたURLをログファイルに書き出し
if len(skipped_urls) > 0:
    with open(skipped_file_path, 'w') as file:
        for url in sorted(skipped_urls):
            file.write(f'{url}\n')
    logging.info('スキップしたURLを「{skipped_file_path}」に記録しました。')

# リトライ用ファイルを書き出し
if retry_urls:
    with open(retry_file_path, 'w') as retry_file:
        for url in sorted(retry_urls):
            retry_file.write(f'{url}\n')
    logging.info(f'一部のWebデータのダウンロードに失敗しました。リトライ用のファイル「{retry_file_path}」を作成しました。')
    print(f'一部のWebデータのダウンロードに失敗しました。リトライ用のファイル「{retry_file_path}」を作成しました。詳細はログファイル「{log_file_path}」を確認してください。')
else:
    if os.path.exists(retry_file_path):
        os.remove(retry_file_path)
    logging.info('全てのWebデータのダウンロードに成功しました。')
    print(f'全てのWebデータのダウンロードに成功しました。詳細はログファイル「{log_file_path}」を確認してください。')
