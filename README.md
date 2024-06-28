# spider_google_play_game_info

1. 生产者（producer.py）
    * 爬取 google play game中，获取每个 game 的 id 。
    * 多线程爬取每一个 game 详情页
    * 将 game info 发送至消息队列
    * 拼接 apkpure下载 url ，多线程下载
2. 消费者（consumer.go）
    * 初始化 mongo 、与 postgre 数据库
    * 接收 kafka 消息、限制最大10个并发 goroutine 来处理消息