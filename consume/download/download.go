package main

import (
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"os"
)

func downloadFile(url string, filepath string) error {
	// 创建文件
	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}
	// 设置伪装的 HTTP 请求头
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
	proxyUrl, _ := neturl.Parse("http://127.0.0.1:7890")
	transport := &http.Transport{
		Proxy: http.ProxyURL(proxyUrl),
	}
	// cookieJar, _ := cookiejar.New(nil)
	client := &http.Client{
		// Jar:       cookieJar,
		Transport: transport,
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// 检查 HTTP 响应状态
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status: %s", resp.Status)
	}

	// 将响应的内容复制到文件中
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}
	return nil
}

func main() {
	url := "https://d.apkpure.com/b/APK/com.supercell.squad?version=latest" // 替换为实际的 APK 下载链接
	filepath := "./apk/test.apk"                                            // 下载的 APK 文件保存路径

	err := downloadFile(url, filepath)
	if err != nil {
		fmt.Printf("Error downloading file: %v\n", err)
	} else {
		fmt.Println("File downloaded successfully!")
	}
}
