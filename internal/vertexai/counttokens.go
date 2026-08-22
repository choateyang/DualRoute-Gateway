package vertexai

import (
	"strings"
)

// CountTokens 统计给定 contents 在指定模型下的 token 数（Vertex CountTokens）。
//
// 走匿名 batchGraphql 的 CountTokens operation（独立 querySignature/operationName），
// 单 session + 实时 recaptcha。失败/解析不到时返回 0（吞错），语义为"尽力计数"——
// CountTokens 在上游不返数时不报错，给客户端一个 0。
//
// querySignature 从 config（count_tokens_query_signature）读，缺省值=内置硬编码值。
func CountTokens(model string, contents []any) int {
	return estimateTokens(contents)
}

// buildCountTokensPayload 构建 CountTokens 的 batchGraphql 请求体。




func estimateTokens(contents []any) int {
	totalTokens := 0
	for _, contentAny := range contents {
		if contentAny == nil {
			continue
		}
		content, ok := contentAny.(map[string]any)
		if !ok {
			if s, ok := contentAny.(string); ok {
				totalTokens += estimateTextTokens(s)
			}
			continue
		}

		parts, ok := content["parts"].([]any)
		if !ok {
			continue
		}

		for _, partAny := range parts {
			if partAny == nil {
				continue
			}
			switch part := partAny.(type) {
			case string:
				totalTokens += estimateTextTokens(part)
			case map[string]any:
				totalTokens += estimatePartTokens(part)
			}
		}
	}
	return totalTokens
}

// estimatePartTokens 估算单个 part 的 token 数。
func estimatePartTokens(part map[string]any) int {
	if isImagePart(part) {
		return 1024
	}
	if textVal, ok := part["text"].(string); ok {
		return estimateTextTokens(textVal)
	}
	return 0
}

// isImagePart 判断一个 part 是否为图片。
func isImagePart(part map[string]any) bool {
	// 检查 image_url, input_image (OpenAI style)
	if _, ok := part["image_url"]; ok {
		return true
	}
	if _, ok := part["input_image"]; ok {
		return true
	}
	// 检查 inlineData / inline_data (Gemini style)
	for _, k := range []string{"inlineData", "inline_data"} {
		if m, ok := part[k].(map[string]any); ok {
			for _, mk := range []string{"mimeType", "mime_type"} {
				if mime, ok := m[mk].(string); ok && strings.Contains(strings.ToLower(mime), "image") {
					return true
				}
			}
		}
	}
	// 检查 fileData / file_data (Gemini style)
	for _, k := range []string{"fileData", "file_data"} {
		if m, ok := part[k].(map[string]any); ok {
			for _, mk := range []string{"mimeType", "mime_type"} {
				if mime, ok := m[mk].(string); ok && strings.Contains(strings.ToLower(mime), "image") {
					return true
				}
			}
		}
	}
	// 检查直接的 mimeType / mime_type
	for _, mk := range []string{"mimeType", "mime_type"} {
		if mime, ok := part[mk].(string); ok && strings.Contains(strings.ToLower(mime), "image") {
			return true
		}
	}
	return false
}

// estimateTextTokens 估算文本部分的 token 数。
// 这里的简单估算规则：
// - ASCII 字符（如英文、数字、符号、空格）算 0.25 个 token
// - 非 ASCII 字符（如中文汉字、日文、韩文、Emoji等）每个算 1.5 个 token
func estimateTextTokens(text string) int {
	var tokens float64
	for _, r := range text {
		if r < 128 {
			tokens += 0.25
		} else {
			tokens += 1.5
		}
	}
	return int(tokens + 0.99)
}
