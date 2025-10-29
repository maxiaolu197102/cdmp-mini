/*
这个包 iputil 提供了获取本地主机 IP 地址和 HTTP 请求中客户端真实 IP 地址的功能。它对于处理网络连接和识别请求来源非常有用。

包摘要
函数名	功能描述	参数	返回值类型	关键特性
GetLocalIP	获取本机的第一个非环回 IPv4 地址。	无	string	遍历所有网络接口，返回第一个有效的私有或公有 IPv4 地址。如果找不到则返回 "127.0.0.1"。
RemoteIP	从 HTTP 请求中解析出客户端的真实 IP 地址。	*http.Request	string	遵循标准 HTTP 头链（X-Client-IP, X-Real-IP, X-Forwarded-For），最后回退到 req.RemoteAddr。
常量:
定义了用于从 HTTP 头部获取 IP 地址的常见键名。

XForwardedFor = "X-Forwarded-For": 标准头部，通常包含代理链中所有节点的 IP 列表。

XRealIP = "X-Real-IP": 非标准但广泛使用的头部，通常由反向代理（如 Nginx）设置，表示客户端的真实 IP。

XClientIP = "x-client-ip": 另一个非标准头部，有时也用于传递客户端 IP。

函数使用方式及示例
1. 获取本地 IP 地址 (GetLocalIP)
用于获取应用程序运行所在机器的网络 IP，而不是环回地址 127.0.0.1。这在需要向外部服务通告自身地址时非常有用（例如，服务注册）。

go
package main

import (
    "fmt"
    "github.com/maxiaolu1981/cretem/nexuscore/component-base/util/iputil"
)

func main() {
    localIP := iputil.GetLocalIP()
    fmt.Printf("Local Machine IP: %s\n", localIP)
    // 输出可能类似于: Local Machine IP: 192.168.1.123
    // 或者在服务器上: Local Machine IP: 10.0.5.27
}
2. 获取客户端远程 IP (RemoteIP)
用于在 HTTP 服务器中获取发起请求的客户端的真实 IP 地址。这在存在负载均衡器或代理时至关重要，因为 req.RemoteAddr 只会包含最后一个代理的地址。

示例：在 HTTP 中间件或处理函数中使用

go
package main

import (
    "fmt"
    "net/http"
    "github.com/maxiaolu1981/cretem/nexuscore/component-base/util/iputil"
)

// 一个简单的中间件，记录请求的客户端 IP
func loggingMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        clientIP := iputil.RemoteIP(r)
        fmt.Printf("Received request from IP: %s for URL: %s\n", clientIP, r.URL.Path)
        next.ServeHTTP(w, r)
    })
}

func mainHandler(w http.ResponseWriter, r *http.Request) {
    clientIP := iputil.RemoteIP(r)
    message := fmt.Sprintf("Hello, your IP is: %s\n", clientIP)
    w.Write([]byte(message))
}

func main() {
    mux := http.NewServeMux()
    mux.HandleFunc("/", mainHandler)

    // 用中间件包装整个路由器
    wrappedMux := loggingMiddleware(mux)

    fmt.Println("Server starting on :8080")
    http.ListenAndServe(":8080", wrappedMux)
}
测试 RemoteIP 的逻辑：
该函数按以下优先级顺序检查 HTTP 头：

X-Client-IP

X-Real-IP

X-Forwarded-For (通常是一个逗号分隔的列表，此函数会取第一个 IP)

如果以上头部都不存在，则回退到 req.RemoteAddr 并去掉端口号。

它还专门处理了 IPv6 环回地址 "::1"，将其转换为 IPv4 的 "127.0.0.1"。

关键注意事项
GetLocalIP 的局限性:

它返回的是第一个找到的非环回 IPv4 地址。如果主机有多个网卡（例如，以太网和 Wi-Fi），它可能无法返回你期望的那个地址。

它不返回 IPv6 地址。

在复杂的网络环境中（如容器、云服务器），获取“正确”的 IP 可能更复杂，有时需要依赖云提供商的元数据服务。

RemoteIP 的安全性:

X-Forwarded-For 等头部极易被伪造。任何客户端或中间的代理都可以设置它们。

此函数隐式信任了这些头部。在生产环境中，这通常只在你的服务器前面有一层你完全信任的反向代理（如 Nginx, Apache, 云负载均衡器）时才安全。这些代理会覆盖或清理这些头部，防止客户端注入虚假 IP。
最佳实践是配置你的反向代理来设置 X-Real-IP 或类似的头部，并且在你的应用代码中只信任来自特定可信代理网络的请求中的这些头部（此函数本身未实现 IP 信任链验证）。
*/

package iputil

import (
	"net"
	"net/http"
)

// Define http headers.
const (
	XForwardedFor = "X-Forwarded-For"
	XRealIP       = "X-Real-IP"
	XClientIP     = "x-client-ip"
)

// GetLocalIP returns the non loopback local IP of the host.
func GetLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "127.0.0.1"
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return "127.0.0.1"
}

// RemoteIP returns the remote ip of the request.
func RemoteIP(req *http.Request) string {
	remoteAddr := req.RemoteAddr
	if ip := req.Header.Get(XClientIP); ip != "" {
		remoteAddr = ip
	} else if ip := req.Header.Get(XRealIP); ip != "" {
		remoteAddr = ip
	} else if ip = req.Header.Get(XForwardedFor); ip != "" {
		remoteAddr = ip
	} else {
		remoteAddr, _, _ = net.SplitHostPort(remoteAddr)
	}

	if remoteAddr == "::1" {
		remoteAddr = "127.0.0.1"
	}

	return remoteAddr
}
