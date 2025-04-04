import requests
from bs4 import BeautifulSoup
import csv
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import os
import subprocess
import logging
import hashlib

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

csv_file = "pornrips_data.csv"
MAX_RETRIES = 3
RETRY_DELAY = 5
COMMIT_INTERVAL = 500  # 每 500 条提交一次

def init_csv():
    """初始化 CSV 文件，如果文件不存在则创建并写入表头"""
    if not os.path.exists(csv_file):
        with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(["page_number", "title", "torrent_link", "magnet_link"])
        logging.info("Initialized new CSV file")
    else:
        logging.info(f"CSV file '{csv_file}' already exists, skipping initialization")

def git_commit(message):
    """提交 CSV 文件到 Git 仓库"""
    try:
        # 配置 Git 身份
        subprocess.run(["git", "config", "--global", "user.email", "hhsw2015@gmail.com"], check=True)
        subprocess.run(["git", "config", "--global", "user.name", "hhsw2015e"], check=True)

        subprocess.run(["git", "add", csv_file], check=True)
        result = subprocess.run(["git", "commit", "-m", message], capture_output=True, text=True)
        if result.returncode == 0:
            subprocess.run(["git", "push"], check=True)
            logging.info(f"Git commit successful: {message}")
        else:
            logging.warning(f"No changes to commit: {result.stderr}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Git error: {e.stderr}")
        raise

def torrent_to_magnet(torrent_url):
    """将种子链接转换为磁力链接（简单实现，假设种子文件可下载）"""
    try:
        response = requests.get(torrent_url, headers=headers, timeout=10)
        response.raise_for_status()
        torrent_content = response.content
        info_hash = hashlib.sha1(torrent_content).hexdigest()
        magnet = f"magnet:?xt=urn:btih:{info_hash}"
        return magnet
    except Exception as e:
        logging.error(f"Failed to convert {torrent_url} to magnet: {e}")
        return "N/A"

def crawl_detail_page(detail_url, page_number, index):
    """爬取详细页面并提取种子链接"""
    try:
        response = requests.get(detail_url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        torrent_link = None
        for a in soup.find_all("a", href=True):
            if "torrents" in a["href"] and a["href"].endswith(".torrent"):
                torrent_link = a["href"]
                break

        if not torrent_link:
            logging.warning(f"No torrent link found on {detail_url}")
            return {"page_number": page_number, "title": "N/A", "torrent_link": "N/A", "magnet_link": "N/A", "index": index}

        title = soup.find("strong", text="Release:").next_sibling.strip() if soup.find("strong", text="Release:") else "N/A"
        magnet_link = torrent_to_magnet(torrent_link)

        logging.info(f"Detail page data - page: {page_number}, title: {title}, torrent: {torrent_link}")
        return {"page_number": page_number, "title": title, "torrent_link": torrent_link, "magnet_link": magnet_link, "index": index}

    except requests.RequestException as e:
        logging.error(f"Error fetching detail page {detail_url}: {e}")
        time.sleep(RETRY_DELAY)
        return {"page_number": page_number, "title": "N/A", "torrent_link": "N/A", "magnet_link": "N/A", "index": index}

def crawl_page(page_number):
    """爬取单个页面"""
    url = f"https://pornrips.to/page/{page_number}/"
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        posts = soup.find_all("div", class_="excerpt-main-content")
        logging.info(f"Page {page_number}: Found {len(posts)} posts")

        sub_tasks = []
        for index, post in enumerate(posts):
            link = post.find("a", href=True, rel="bookmark")
            if link:
                sub_tasks.append((link["href"], page_number, index))

        results = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(crawl_detail_page, url, pid, idx) for url, pid, idx in sub_tasks]
            for future in as_completed(futures):
                result = future.result()
                if result:
                    results.append(result)

        results.sort(key=lambda x: x["index"])  # 按页面顺序排序
        return results

    except requests.RequestException as e:
        logging.error(f"Error fetching page {url}: {e}")
        return []

def crawl_pages(start_page, end_page):
    """主爬取逻辑"""
    init_csv()
    pbar = tqdm(range(start_page, end_page - 1, -1), desc="Crawling pages")
    total_records = 0

    for page_number in pbar:
        results = crawl_page(page_number)
        if results:
            with open(csv_file, mode='a', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                for data in results:
                    writer.writerow([data["page_number"], data["title"], data["torrent_link"], data["magnet_link"]])
                    total_records += 1

            if total_records >= COMMIT_INTERVAL:
                git_commit(f"Update data for {total_records} records up to page {page_number}")
                total_records = 0  # 重置计数器

        time.sleep(1)  # 防止过于频繁请求

    if total_records > 0:
        git_commit(f"Final update for remaining {total_records} records")

if __name__ == "__main__":
    logging.info("Starting crawl...")
    start_page = int(os.getenv("START_PAGE", 5946))
    end_page = int(os.getenv("END_PAGE", 1))
    crawl_pages(start_page, end_page)
    logging.info(f"Data saved to {csv_file}")
