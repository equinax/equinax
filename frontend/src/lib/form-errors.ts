// FastAPI 422 错误格式
interface FastAPIValidationError {
  loc: (string | number)[]
  msg: string
  type: string
}

interface APIErrorResponse {
  response?: {
    data?: {
      detail?: FastAPIValidationError[] | string
    }
  }
  message?: string
}

// 字段名映射（API snake_case -> 表单 camelCase）
const fieldNameMap: Record<string, string> = {
  name: 'name',
  code: 'code',
  description: 'description',
  strategy_type: 'strategyType',
  indicators_used: 'indicatorsUsed',
}

export interface ParsedAPIError {
  fieldErrors: Record<string, string>
  generalError: string | null
}

/**
 * 解析 FastAPI 422 错误响应，提取字段级错误
 */
export function parseAPIError(error: unknown): ParsedAPIError {
  const apiError = error as APIErrorResponse
  const fieldErrors: Record<string, string> = {}
  let generalError: string | null = null

  try {
    const detail = apiError?.response?.data?.detail

    if (Array.isArray(detail)) {
      detail.forEach((err) => {
        // 获取字段名（通常是 loc 数组的最后一个元素）
        const apiFieldName = err.loc?.[err.loc.length - 1]
        if (typeof apiFieldName === 'string') {
          const formFieldName = fieldNameMap[apiFieldName] || apiFieldName
          fieldErrors[formFieldName] = err.msg
        }
      })

      // 如果没有提取到字段错误，合并为通用错误
      if (Object.keys(fieldErrors).length === 0) {
        generalError = detail.map((e) => e.msg).join('; ')
      }
    } else if (typeof detail === 'string') {
      generalError = detail
    } else if (apiError?.message) {
      generalError = apiError.message
    } else {
      generalError = '保存失败，请稍后重试'
    }
  } catch {
    generalError = '保存失败，请稍后重试'
  }

  return { fieldErrors, generalError }
}
