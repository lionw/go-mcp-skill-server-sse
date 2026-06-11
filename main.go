package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

// ====================== 1. 定义Skill的核心常量与资源 ======================
const taxRule2026 = `
2026年个人所得税新规：
1. 个税起征点：5000元/月
2. 税率表（应纳税所得额=税前工资-5000-五险一金）：
   - 不超过3000元：3%
   - 3000-12000元：10%，速算扣除数210
   - 12000-25000元：20%，速算扣除数1410
   - 25000-35000元：25%，速算扣除数2660
3. 五险一金扣除比例：固定为工资的10.5%
`

// 修复原模板：由于要用 strings.Replacer 替换 {{变量}}，这里保持原样
const salarySlipTemplate = `
请根据以下核算结果，生成一份标准的员工月度工资单：
员工姓名：{{name}}
核算月份：{{month}}
税前工资：{{base_salary}}元
考勤扣款：{{attendance_deduct}}元
五险一金扣除：{{social_security}}元
个税扣除：{{tax}}元
实发工资：{{final_salary}}元
要求：格式清晰，数据准确，无多余内容。
`

// ====================== 2. 标准 JSON-RPC 协议结构体 ======================
type JsonRpcRequest struct {
	JsonRpc string                 `json:"jsonrpc"`
	Method  string                 `json:"method"`
	Params  map[string]interface{} `json:"params,omitempty"`
	Id      interface{}            `json:"id,omitempty"`
}

type JsonRpcResponse struct {
	JsonRpc string      `json:"jsonrpc"`
	Id      interface{} `json:"id"`
	Result  interface{} `json:"result,omitempty"`
	Error   interface{} `json:"error,omitempty"`
}

type SseSession struct {
	mu       sync.Mutex
	writeCh  chan []byte
	isClosed bool
}

var currentSession *SseSession

// ====================== 3. 强力 CORS 跨域中间件 ======================
func corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "*")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next(w, r)
	}
}

// ====================== 4. 主函数与端口监听 ======================
func main() {
	mux := http.NewServeMux()

	mux.HandleFunc("/sse", corsMiddleware(handleSseConnect))
	mux.HandleFunc("/sse/msg", corsMiddleware(handleSseMessage))

	log.Println("🚀 员工工资核算Skill已启动，HTTP/SSE 模式运行中...")
	log.Println("📡 正在监听 Hugging Face 专属端口 :7860")

	// 🌟 必须监听 7860 端口以适配 Hugging Face 托管环境
	if err := http.ListenAndServe("0.0.0.0:7860", mux); err != nil {
		log.Fatalf("服务启动失败: %v", err)
	}
}

// ====================== 5. SSE 通道连接管理 ======================
func handleSseConnect(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	session := &SseSession{
		writeCh: make(chan []byte, 100),
	}
	currentSession = session

	// 🌟 动态识别 Hugging Face 公网域名
	scheme := "http"
	if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" {
		scheme = "https"
	}
	fmt.Fprintf(w, "event: endpoint\ndata: %s://%s/sse/msg\n\n", scheme, r.Host)
	flusher.Flush()

	for {
		select {
		case data, ok := <-session.writeCh:
			if !ok {
				return
			}
			fmt.Fprintf(w, "event: message\ndata: %s\n\n", string(data))
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

func handleSseMessage(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Read body failed", http.StatusBadRequest)
		return
	}

	var req JsonRpcRequest
	if err := json.Unmarshal(body, &req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte("accepted"))

	go processMcpRequest(req)
}

// ====================== 6. 原子化 Tool 业务逻辑 ======================
func queryAttendanceTool(employeeName, month string) float64 {
	return map[string]float64{
		"张三": 200,
		"李四": 0,
		"王五": 500,
	}[employeeName]
}

func calculateTaxTool(baseSalary float64) float64 {
	socialSecurity := baseSalary * 0.105
	taxableIncome := baseSalary - socialSecurity - 5000

	var tax float64
	switch {
	case taxableIncome <= 0:
		tax = 0
	case taxableIncome <= 3000:
		tax = taxableIncome * 0.03
	case taxableIncome <= 12000:
		tax = taxableIncome*0.1 - 210
	case taxableIncome <= 25000:
		tax = taxableIncome*0.2 - 1410
	default:
		tax = taxableIncome*0.25 - 2660
	}

	if tax < 0 {
		tax = 0
	}
	return tax
}

// ====================== 7. MCP 核心协议解析器 ======================
func processMcpRequest(req JsonRpcRequest) {
	var response JsonRpcResponse
	response.JsonRpc = "2.0"
	response.Id = req.Id

	switch req.Method {
	case "initialize":
		// 对应原代码 capabilities 声明，同时开启 tools 和 resources 支持
		response.Result = map[string]interface{}{
			"protocolVersion": "2024-11-05",
			"capabilities": map[string]interface{}{
				"tools":     map[string]interface{}{},
				"resources": map[string]interface{}{},
			},
			"serverInfo": map[string]interface{}{
				"name":    "salary-calculation-skill",
				"version": "1.0.0",
			},
		}

	case "resources/list":
		// 对应原代码：server.RegisterResource 声明
		response.Result = map[string]interface{}{
			"resources": []map[string]interface{}{
				{
					"uri":         "https://company.com/tax/2026",
					"name":        "2026年个人所得税新规",
					"description": "2026年度最新的起征点、税率表与社保扣除固定比例",
					"mimeType":    "text/plain",
				},
			},
		}

	case "resources/read":
		// 对应原代码：读取个税 Resource 的回调闭包
		uri, _ := req.Params["uri"].(string)
		if uri == "https://company.com/tax/2026" {
			response.Result = map[string]interface{}{
				"contents": []map[string]interface{}{
					{
						"uri":      uri,
						"mimeType": "text/plain",
						"text":     taxRule2026,
					},
				},
			}
		}

	case "tools/list":
		// 对应原代码：server.RegisterTool 的入参 Schema 声明
		response.Result = map[string]interface{}{
			"tools": []map[string]interface{}{
				{
					"name":        "salary_calculation_skill",
					"description": "员工月度工资核算Skill，用于根据员工的姓名、核算月份、税前基本工资，完成完整的工资核算，包括考勤扣款、五险一金、个税计算，最终生成标准的员工工资单。仅用于员工月度工资核算场景，不要用于其他场景。",
					"inputSchema": map[string]interface{}{
						"type":     "object",
						"required": []string{"employee_name", "month", "base_salary"},
						"properties": map[string]interface{}{
							"employee_name": map[string]interface{}{"type": "string", "description": "员工姓名，必填"},
							"month":         map[string]interface{}{"type": "string", "description": "核算月份，格式为YYYY-MM，必填"},
							"base_salary":   map[string]interface{}{"type": "string", "description": "员工税前月基本工资，必填"},
						},
					},
				},
			},
		}

	case "tools/call":
		// 对应原代码：salaryCalculationSkillHandler 核心控制流
		toolName, _ := req.Params["name"].(string)
		arguments, _ := req.Params["arguments"].(map[string]interface{})

		if toolName == "salary_calculation_skill" {
			// 步骤1：入参校验
			employeeName, ok := arguments["employee_name"].(string)
			if !ok || employeeName == "" {
				sendErrorResponse(req.Id, "员工姓名为必填参数，不能为空")
				return
			}

			month, ok := arguments["month"].(string)
			if !ok || month == "" {
				sendErrorResponse(req.Id, "核算月份为必填参数，不能为空")
				return
			}

			baseSalaryStr, ok := arguments["base_salary"].(string)
			if !ok || baseSalaryStr == "" {
				sendErrorResponse(req.Id, "税前基本工资为必填参数，不能为空")
				return
			}
			baseSalary, err := strconv.ParseFloat(baseSalaryStr, 64)
			if err != nil || baseSalary <= 0 {
				sendErrorResponse(req.Id, "税前基本工资必须是大于0的数字")
				return
			}

			// 步骤2：按固化流程执行任务
			attendanceDeduct := queryAttendanceTool(employeeName, month)
			socialSecurity := baseSalary * 0.105
			tax := calculateTaxTool(baseSalary - attendanceDeduct)
			finalSalary := baseSalary - attendanceDeduct - socialSecurity - tax

			// 步骤3：结果校验
			if finalSalary < 0 {
				sendErrorResponse(req.Id, "工资核算异常，实发工资不能为负数，请检查输入参数")
				return
			}

			// 步骤4：用模板格式化结果（还原原代码里的 {{变量}} 字符串替换逻辑）
			replacer := strings.NewReplacer(
				"{{name}}", employeeName,
				"{{month}}", month,
				"{{base_salary}}", fmt.Sprintf("%.2f", baseSalary),
				"{{attendance_deduct}}", fmt.Sprintf("%.2f", attendanceDeduct),
				"{{social_security}}", fmt.Sprintf("%.2f", socialSecurity),
				"{{tax}}", fmt.Sprintf("%.2f", tax),
				"{{final_salary}}", fmt.Sprintf("%.2f", finalSalary),
			)
			slip := replacer.Replace(salarySlipTemplate)

			// 步骤5：记录审计日志（改用标准的 Go log 打印，云端控制台可查）
			log.Printf("[审计日志] 员工：%s，月份：%s，核算完成，实发工资：%.2f\n", employeeName, month, finalSalary)

			response.Result = map[string]interface{}{
				"content": []map[string]interface{}{
					{"type": "text", "text": slip},
				},
			}
		}
	}

	if currentSession != nil {
		respBytes, _ := json.Marshal(response)
		respBytes = bytes.ReplaceAll(respBytes, []byte("\n"), []byte(""))
		currentSession.writeCh <- respBytes
	}
}

func sendErrorResponse(id interface{}, msg string) {
	if currentSession == nil {
		return
	}
	var resp JsonRpcResponse
	resp.JsonRpc = "2.0"
	resp.Id = id
	resp.Error = map[string]interface{}{"code": -32603, "message": msg}
	respBytes, _ := json.Marshal(resp)
	currentSession.writeCh <- respBytes
}
