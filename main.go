package main

import (
	"bufio"
	"cache1go/cache"
	"cache1go/nutsdb"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
)


//const STORE_MODE = "nutsdb"
const STORE_MODE = "in-memory"

func main() {

	// 1.监听
	//调用 Go 语言 net包 的 Listen 函数监听本机 TCP 的 9998 端口
	listener, err := net.Listen("tcp", ":9998")
	if err != nil {
		log.Fatalf("listen error: %v", err)
	}
	log.Printf("Server started at %s", "9998")

	defer listener.Close()

	// 2.接收一个client

	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}

		log.Println("New Client Connected : ", conn.RemoteAddr())

		// 3. 给每个client请求都分配一个goroutine

		go handleRequest(conn)
	}
}

var Xcache = cache.New()
// 处理接收到的connection
func handleRequest(conn net.Conn) {


	defer func() {
		log.Println("Client Disconnected :" + conn.RemoteAddr().String())
		conn.Close()
	}()

	for {
		message, error := bufio.NewReader(conn).ReadString('\n')

		fmt.Println(message)

		if message == "ping\n" || message == "PING\n" {
			ret := "PONG"
			conn.Write([]byte(ret + "\n"))
			log.Println("Send To Client : ", ret)
			return
		}

		if error!=nil { //如client连接断开,这里不能使用log.Fatal,server进程要常驻
			log.Println(error)
			return
		}else {
			if message != "\n" { //过滤掉客户端换行无内容类型
				// output message received
				log.Println("Message Received : ", message)
				// send new string back to client

				ret := handle(message)
				log.Println("Send To Client : ", ret)
				conn.Write([]byte(ret + "\n"))
			}

		}
	}

}



func handle(message string) string {
	//set cn 中国 -1 \n => [set cn 中国 -1]
	order := strings.Split(strings.Replace(string(message), "\n", "", -1), " ")
	command := order[0]
	log.Println("use store_mode: ", STORE_MODE)

	ret := ""
	switch command {
		case "set":
			ret = write(order)
		case "get":
			ret = read(order)
	}

	return ret

}

func write(order []string) string{

	//TODO.. set方式标准化判断
	key:=order[1]
	value := order[2]

	expireAt, paserErr := strconv.ParseInt(order[3], 10, 64)
	if paserErr!=nil {
		return "invalid syntax"
	}
	Xcache.Set(key, value, expireAt)


	if STORE_MODE == "nutsdb" {
		nutsdb := nutsdb.Nuts{}
		if expireAt == -1 {
			expireAt = 0
		}
		nutsdb.Write([]byte(key),[]byte(value), uint32(expireAt))
		//nutsdb.Close()
	}

	return "OK"
}

func read(order []string) string{
	value := Xcache.Get(order[1])
	ret := "(nil)"
	if value != nil {
		ret = value.(string)
	}else {
		//如果内存无数据,则查询nutsdb并返回
		if STORE_MODE == "nutsdb" {
			nutsdb := nutsdb.Nuts{}
			ret = nutsdb.Read([]byte(order[1]))
			//nutsdb.Close()
		}
	}

	return ret
}