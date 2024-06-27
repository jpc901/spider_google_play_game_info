import re
import os.path
import socket
import requests
import json
import threading
from confluent_kafka import Producer
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup

header = {'User-Agent': 'Mozilla/5.0 (compatible; Baiduspider-render/2.0; +http://www.baidu.com/search/spider.html)'}
download_header = { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/605.1.15      (KHTML, like Gecko) Version/13.1 Safari/605.1.15' }
google_base_url = "https://play.google.com"
google_play_game_url = 'https://play.google.com/store/games'
apk_base_url = "https://d.apkpure.com/b/APK/"
apk_path = "./apk"
count = 0
download_num = 3
# 获取game详细信息的线程
spider_threads = []
# 控制共享资源变量
lock = threading.Lock()
download_url_list = []
s = requests.Session()
s.mount('http://', HTTPAdapter(max_retries=3))
s.mount('https://', HTTPAdapter(max_retries=3))

# 代理
proxys = {
    "http": "http://0.0.0.0:7890"
}
# 配置生产者参数
conf = {
    'bootstrap.servers': 'localhost:9092',  # Kafka 服务器地址
    'client.id': socket.gethostname()
}

# 创建生产者实例
producer = Producer(**conf)

# 发送消息的回调函数，确认消息是否发送成功
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# 消息发送到消息队列
def send_msg(message):
    topic = 'gameinfo'
    # 发送消息
    producer.produce(topic, key='key', value=message, callback=delivery_report)

    # 确保所有消息都被发送
    producer.flush()

def get_apk_url(pkg_name):
    return apk_base_url + pkg_name + "?version=latest"


def get_gameinfo(game_url, pkg_name):
    global count
    global download_num
    global lock
    global download_url_list
    try:
        r = s.get(game_url, headers=header,proxies=proxys)
        # 游戏详情的响应
        detail_resp = r.text
        # 解析
        soup1 = BeautifulSoup(detail_resp, "html.parser")
        gameinfo = {}
        # 解析游戏名称
        name_soup = soup1.find_all(name='h1', attrs={"itemprop": "name"})
        game_name = re.findall("<h1[^>]*>(.*?)</h1>", str(name_soup[0]))[0]
        gameinfo["name"] = game_name
        # 解析游戏图片
        avatar_soup = soup1.find_all(name='img', attrs={"class": "T75of cN0oRe fFmL2e"})[0]
        gameinfo["avatar"] = avatar_soup['src']
        # 解析游戏出版公司
        company_soup = soup1.find_all(name='div', attrs={"class": "Vbfug auoIOc"})
        gameinfo["company"] = re.findall("<span>(.*?)</span>", str(company_soup[0]))[0]
        # 解析下载次数
        download_soup = soup1.find_all(name='div', attrs={"class": "ClM7O"})
        gameinfo["download_num"] = re.findall(">(.*?)<", str(download_soup[0]))[0]
        # 解析游戏简介
        desc_soup = soup1.find_all(name='div', attrs={"class": "bARER"})[0]
        gameinfo["description"] = desc_soup.text
        # 构建下载apk包url
        apk_url = get_apk_url(pkg_name)
        gameinfo["apk_url"] = apk_url
        count += 1
        
        lock.acquire()
        try: 
            if download_num > 0:
                download_num -= 1
                download_url_list.append((apk_url, game_name))
        finally:
            lock.release()
    except Exception as e:
        print(e)

    # 发送消息到消息队列
    json_gameinfo = json.dumps(gameinfo, ensure_ascii=False, indent=4)
    send_msg(json_gameinfo)
    
def download_apk(download_url,apk_name):
    print(u"开始下载:%s.apk\n" % apk_name)
    r = s.get(url=download_url, headers=download_header,proxies=proxys)

    f_path = os.path.join(apk_path,apk_name+".apk")
    
    with open(f_path, 'wb') as writer:
        writer.write(r.content)


if __name__ == '__main__':
    # 爬取google play中游戏url、id
    print("开始爬虫")
    
    # 伪装header
    r = s.get(google_play_game_url, headers=header,proxies=proxys)
    # 游戏页的响应
    index_resp = r.text
    # 解析
    soup = BeautifulSoup(index_resp, "html.parser")
    game_infos = soup.find_all(name="a", attrs={"class": "Si6A0c Gy4nib"})
    for info in game_infos:
        # 得到游戏详情页的url
        game_detail_url = google_base_url + info['href']
        pkg_name = re.findall("id=(.*?)$", game_detail_url)[0]
        # 去重
        print(f"开始爬取,url:{game_detail_url} ,apk包名:{pkg_name}")
        # 并发通过游戏url查询详情获取gameinfo
        t = threading.Thread(target=get_gameinfo,args=(game_detail_url,pkg_name))
        spider_threads.append(t)
        t.start()
        # get_gameinfo(game_detail_url, pkg_name)

    
    for t in spider_threads:
        t.join()
        
    # print(download_url_list)
    # 并发执行下载任务
    download_threads = []
    for game_download_info in download_url_list:
        t = threading.Thread(target=download_apk,args=(game_download_info[0],game_download_info[1]))
        t.start()
        download_threads.append(t)
    
    for t in download_threads:
        t.join()

    print(f"共{count}条数据")