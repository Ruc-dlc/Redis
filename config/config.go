/**
 * 配置文件解析模块
 * 
 * 本文件实现了Redis风格的配置文件解析功能，主要功能包括：
 * 1. 解析配置文件并加载服务器属性
 * 2. 提供默认配置值
 * 3. 支持动态配置更新
 * 4. 处理集群和单机模式配置
 * 
 * 配置文件格式支持：
 * - 键值对配置(key value)
 * - #开头的注释行
 * - 自动类型转换(字符串、整数、布尔值)
 */

 package config

 import (
	 "bufio"
	 "io"
	 "os"
	 "path/filepath"
	 "reflect"
	 "strconv"
	 "strings"
	 "time"
 
	 "redis/myredis/lib/utils"
	 "redis/myredis/lib/logger"
 )
 
 // 运行模式常量
 var (
	 ClusterMode    = "cluster"    // 集群模式
	 StandaloneMode = "standalone" // 单机模式
 )
 
 // ServerProperties 服务器配置属性
 type ServerProperties struct {
	 RunID             string `cfg:"runid"`              // 服务器运行ID
	 Bind              string `cfg:"bind"`               // 绑定地址
	 Port              int    `cfg:"port"`               // 监听端口
	 Dir               string `cfg:"dir"`                // 工作目录
	 AnnounceHost      string `cfg:"announce-host"`      // 对外宣布的主机名
	 AppendOnly        bool   `cfg:"appendonly"`         // 是否开启AOF持久化
	 AppendFilename    string `cfg:"appendfilename"`     // AOF文件名
	 AppendFsync       string `cfg:"appendfsync"`        // AOF同步策略
	 AofUseRdbPreamble bool   `cfg:"aof-use-rdb-preamble"` // 是否使用RDB前导
	 MaxClients        int    `cfg:"maxclients"`         // 最大客户端连接数
	 RequirePass       string `cfg:"requirepass"`        // 认证密码
	 Databases         int    `cfg:"databases"`          // 数据库数量
	 RDBFilename       string `cfg:"dbfilename"`         // RDB文件名
	 MasterAuth        string `cfg:"masterauth"`         // 主节点认证密码
	 SlaveAnnouncePort int    `cfg:"slave-announce-port"` // 从节点宣布端口
	 SlaveAnnounceIP   string `cfg:"slave-announce-ip"`  // 从节点宣布IP
	 ReplTimeout       int    `cfg:"repl-timeout"`       // 复制超时时间(秒)
 
	 // 集群相关配置
	 ClusterEnable     bool   `cfg:"cluster-enable"`     // 是否启用集群
	 ClusterAsSeed     bool   `cfg:"cluster-as-seed"`    // 是否作为种子节点
	 ClusterSeed       string `cfg:"cluster-seed"`       // 集群种子节点地址
	 RaftListenAddr    string `cfg:"raft-listen-address"` // Raft监听地址
	 RaftAdvertiseAddr string `cfg:"raft-advertise-address"` // Raft广播地址
	 
	 // 非标准配置
	 CfPath string `cfg:"cf,omitempty"` // 配置文件路径(内部使用)
 }
 
 // ServerInfo 服务器运行时信息
 type ServerInfo struct {
	 StartUpTime time.Time // 服务器启动时间
 }
 
 // AnnounceAddress 获取服务器对外宣布的地址
 func (p *ServerProperties) AnnounceAddress() string {
	 if p.AnnounceHost != "" {
		 return p.AnnounceHost + ":" + strconv.Itoa(p.Port)
	 }
	 return p.Bind + ":" + strconv.Itoa(p.Port)
 }
 
 // 全局变量
 var (
	 Properties          *ServerProperties // 全局配置属性
	 EachTimeServerInfo  *ServerInfo       // 服务器运行时信息
 )
 
 // 初始化默认配置
 func init() {
	 EachTimeServerInfo = &ServerInfo{
		 StartUpTime: time.Now(), // 记录启动时间
	 }
 
	 // 设置默认配置值
	 Properties = &ServerProperties{
		 Bind:       "127.0.0.1", // 默认绑定本地回环地址
		 Port:       6379,        // 默认Redis端口
		 AppendOnly: false,       // 默认关闭AOF
		 RunID:      utils.RandString(40), // 生成随机运行ID
	 }
 }
 
 // parse 解析配置文件内容
 func parse(src io.Reader) *ServerProperties {
	 config := &ServerProperties{}
 
	 // 读取原始配置到map
	 rawMap := make(map[string]string)
	 scanner := bufio.NewScanner(src)
	 for scanner.Scan() {
		 line := scanner.Text()
		 // 跳过注释行
		 if len(line) > 0 && strings.TrimLeft(line, " ")[0] == '#' {
			 continue
		 }
		 // 解析键值对
		 pivot := strings.IndexAny(line, " ")
		 if pivot > 0 && pivot < len(line)-1 { 
			 key := line[0:pivot]
			 value := strings.Trim(line[pivot+1:], " ")
			 rawMap[strings.ToLower(key)] = value // 统一转为小写
		 }
	 }
	 if err := scanner.Err(); err != nil {
		 logger.Fatal(err)
	 }
 
	 // 使用反射将配置值设置到结构体
	 t := reflect.TypeOf(config)
	 v := reflect.ValueOf(config)
	 n := t.Elem().NumField()
	 for i := 0; i < n; i++ {
		 field := t.Elem().Field(i)
		 fieldVal := v.Elem().Field(i)
		 // 获取cfg标签作为配置键名
		 key, ok := field.Tag.Lookup("cfg")
		 if !ok || strings.TrimLeft(key, " ") == "" {
			 key = field.Name // 默认使用字段名
		 }
		 // 从配置map中获取值
		 value, ok := rawMap[strings.ToLower(key)]
		 if ok {
			 // 根据字段类型进行转换
			 switch field.Type.Kind() {
			 case reflect.String:
				 fieldVal.SetString(value)
			 case reflect.Int:
				 intValue, err := strconv.ParseInt(value, 10, 64)
				 if err == nil {
					 fieldVal.SetInt(intValue)
				 }
			 case reflect.Bool:
				 boolValue := ("yes" == value) // Redis风格布尔值(yes/no)
				 fieldVal.SetBool(boolValue)
			 case reflect.Slice:
				 if field.Type.Elem().Kind() == reflect.String {
					 slice := strings.Split(value, ",") // 逗号分隔的字符串列表
					 fieldVal.Set(reflect.ValueOf(slice))
				 }
			 }
		 }
	 }
	 return config
 }
 
 // SetupConfig 加载并设置配置文件
 func SetupConfig(configFilename string) {
	 file, err := os.Open(configFilename)
	 if err != nil {
		 panic(err)
	 }
	 defer file.Close()
	 
	 // 解析配置文件
	 Properties = parse(file)
	 // 生成随机运行ID
	 Properties.RunID = utils.RandString(40)
	 
	 // 记录配置文件绝对路径
	 configFilePath, err := filepath.Abs(configFilename)
	 if err != nil {
		 return
	 }
	 Properties.CfPath = configFilePath
	 
	 // 设置默认工作目录
	 if Properties.Dir == "" {
		 Properties.Dir = "."
	 }
 }
 
 // GetTmpDir 获取临时目录路径
 func GetTmpDir() string {
	 return Properties.Dir + "/tmp"
 }