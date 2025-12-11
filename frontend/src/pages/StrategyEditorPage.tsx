import { useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useForm } from '@tanstack/react-form'
import { useQueryClient } from '@tanstack/react-query'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import {
  Save,
  Play,
  ArrowLeft,
  Loader2,
  CheckCircle,
  XCircle,
  Copy,
  Check,
} from 'lucide-react'
import {
  useGetStrategyApiV1StrategiesStrategyIdGet,
  useCreateStrategyApiV1StrategiesPost,
  useUpdateStrategyApiV1StrategiesStrategyIdPut,
  useValidateCodeInlineApiV1StrategiesValidateCodePost,
  useListStrategyTemplatesApiV1StrategiesTemplatesListGet,
} from '@/api/generated/strategies/strategies'
import {
  defaultStrategyValues,
  strategyTypeOptions,
  type StrategyFormValues,
} from '@/lib/schemas/strategy'
import { parseAPIError } from '@/lib/form-errors'
import { cn } from '@/lib/utils'
import type { StrategyResponse } from '@/api/generated/schemas/strategyResponse'

type SaveStatus = 'idle' | 'saving' | 'success' | 'error'

// 从 TanStack Form 错误数组中提取错误消息
function getErrorMessage(errors: unknown[]): string {
  return errors
    .map((err) => {
      if (typeof err === 'string') return err
      if (err && typeof err === 'object' && 'message' in err) {
        return (err as { message: string }).message
      }
      return ''
    })
    .filter(Boolean)
    .join(', ')
}

// 内部表单组件 - 只有数据准备好后才渲染
function StrategyEditorForm({
  strategy,
  isEditing,
}: {
  strategy: StrategyResponse | undefined
  isEditing: boolean
}) {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const strategyId = strategy?.id

  // 非表单状态
  const [saveStatus, setSaveStatus] = useState<SaveStatus>('idle')
  const [apiError, setApiError] = useState<string | null>(null)
  const [validationResult, setValidationResult] = useState<{
    is_valid: boolean
    error_message?: string | null
    errors?: string[]
  } | null>(null)
  const [indicatorInput, setIndicatorInput] = useState('')

  // 表单初始化 - 直接使用 strategy 数据作为默认值
  const form = useForm({
    defaultValues: strategy
      ? {
          name: strategy.name,
          description: strategy.description || '',
          strategyType: (strategy.strategy_type as StrategyFormValues['strategyType']) || 'other',
          code: strategy.code,
          indicatorsUsed: strategy.indicators_used || [],
        }
      : defaultStrategyValues,
  })

  // 获取模板
  const { data: templates } = useListStrategyTemplatesApiV1StrategiesTemplatesListGet()

  // Mutations
  const createMutation = useCreateStrategyApiV1StrategiesPost()
  const updateMutation = useUpdateStrategyApiV1StrategiesStrategyIdPut()
  const validateMutation = useValidateCodeInlineApiV1StrategiesValidateCodePost()

  // 清除所有字段的服务端错误
  const clearServerErrors = () => {
    const fieldNames: (keyof StrategyFormValues)[] = ['name', 'description', 'strategyType', 'code', 'indicatorsUsed']
    fieldNames.forEach((fieldName) => {
      form.setFieldMeta(fieldName, (prev) => ({
        ...prev,
        errorMap: {
          ...prev.errorMap,
          onServer: undefined,
        },
      }))
    })
  }

  // 处理保存
  const handleSave = async () => {
    // 手动验证所有字段
    const nameValue = form.state.values.name
    const codeValue = form.state.values.code

    let hasErrors = false

    if (nameValue.length < 3) {
      form.setFieldMeta('name', (prev) => ({
        ...prev,
        errors: ['策略名称至少需要3个字符'],
      }))
      hasErrors = true
    }

    if (!codeValue || codeValue.length === 0) {
      form.setFieldMeta('code', (prev) => ({
        ...prev,
        errors: ['策略代码不能为空'],
      }))
      hasErrors = true
    }

    if (hasErrors) {
      return
    }

    const values = form.state.values
    setApiError(null)
    clearServerErrors()
    setSaveStatus('saving')

    const payload = {
      name: values.name,
      description: values.description || null,
      strategy_type: values.strategyType,
      code: values.code,
      indicators_used: values.indicatorsUsed,
    }

    try {
      if (isEditing && strategyId) {
        await updateMutation.mutateAsync({
          strategyId: strategyId,
          data: payload,
        })
        queryClient.invalidateQueries({ queryKey: ['/api/v1/strategies'] })
        queryClient.invalidateQueries({
          queryKey: [`/api/v1/strategies/${strategyId}`],
        })
        setSaveStatus('success')
        setValidationResult(null)
        setTimeout(() => setSaveStatus('idle'), 2000)
      } else {
        const data = await createMutation.mutateAsync({ data: payload })
        queryClient.invalidateQueries({ queryKey: ['/api/v1/strategies'] })
        setSaveStatus('success')
        setValidationResult(null)
        setTimeout(() => navigate(`/strategies/${data.id}`), 800)
      }
    } catch (error) {
      setSaveStatus('error')
      const { fieldErrors, generalError } = parseAPIError(error)

      if (Object.keys(fieldErrors).length > 0) {
        // 设置字段级错误
        Object.entries(fieldErrors).forEach(([fieldName, errorMessage]) => {
          form.setFieldMeta(fieldName as keyof StrategyFormValues, (prev) => ({
            ...prev,
            errorMap: {
              ...prev.errorMap,
              onServer: errorMessage,
            },
          }))
        })
        setApiError(null)
      } else {
        setApiError(generalError)
      }
    }
  }

  // 处理代码验证
  const handleValidate = () => {
    const code = form.state.values.code
    validateMutation.mutate(
      { data: { code } },
      {
        onSuccess: (data) => {
          setValidationResult(data)
          if (saveStatus === 'success') {
            setSaveStatus('idle')
          }
        },
      }
    )
  }

  // 处理指标添加
  const handleAddIndicator = () => {
    const trimmed = indicatorInput.trim()
    if (trimmed) {
      const current = form.state.values.indicatorsUsed
      if (!current.includes(trimmed)) {
        form.setFieldValue('indicatorsUsed', [...current, trimmed])
      }
      setIndicatorInput('')
    }
  }

  // 处理指标移除
  const handleRemoveIndicator = (indicator: string) => {
    const current = form.state.values.indicatorsUsed
    form.setFieldValue(
      'indicatorsUsed',
      current.filter((i) => i !== indicator)
    )
  }

  // 使用模板
  const handleUseTemplate = (templateCode: string) => {
    form.setFieldValue('code', templateCode)
    setValidationResult(null)
  }

  // 处理用户编辑时重置状态
  const handleFieldChange = (fieldName?: keyof StrategyFormValues) => {
    if (saveStatus === 'success' || saveStatus === 'error') {
      setSaveStatus('idle')
      setApiError(null)
    }
    // 清除该字段的服务端错误
    if (fieldName) {
      form.setFieldMeta(fieldName, (prev) => ({
        ...prev,
        errorMap: {
          ...prev.errorMap,
          onServer: undefined,
        },
      }))
    }
  }

  const isSaving = saveStatus === 'saving'

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-4">
          <Button variant="ghost" size="icon" onClick={() => navigate('/strategies')}>
            <ArrowLeft className="h-5 w-5" />
          </Button>
          <div>
            <h1 className="text-3xl font-bold">
              {isEditing ? '编辑策略' : '新建策略'}
            </h1>
            <p className="text-muted-foreground">
              {isEditing ? `编辑 ${strategy?.name}` : '创建新的量化交易策略'}
            </p>
          </div>
        </div>
        <div className="flex gap-2">
          <Button
            type="button"
            variant="outline"
            onClick={handleValidate}
            disabled={validateMutation.isPending}
          >
            {validateMutation.isPending ? (
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
            ) : (
              <Play className="mr-2 h-4 w-4" />
            )}
            验证代码
          </Button>
          <Button
            type="button"
            onClick={handleSave}
            disabled={isSaving}
            className={
              saveStatus === 'success'
                ? 'bg-green-500 hover:bg-green-500/90'
                : undefined
            }
          >
            {isSaving ? (
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
            ) : saveStatus === 'success' ? (
              <Check className="mr-2 h-4 w-4" />
            ) : (
              <Save className="mr-2 h-4 w-4" />
            )}
            {saveStatus === 'success' ? '已保存' : '保存策略'}
          </Button>
        </div>
      </div>

      <div className="grid gap-6 lg:grid-cols-3">
        {/* 左侧：代码编辑器 */}
        <div className="lg:col-span-2 space-y-4">
          {/* 保存成功反馈 */}
          {saveStatus === 'success' && (
            <div className="flex items-center gap-2 rounded-lg border border-green-500/50 bg-green-500/10 text-green-500 p-3">
              <CheckCircle className="h-5 w-5" />
              <span>策略保存成功</span>
            </div>
          )}

          {/* API 错误反馈 */}
          {apiError && (
            <div className="flex items-center gap-2 rounded-lg border border-red-500/50 bg-red-500/10 text-red-500 p-3">
              <XCircle className="h-5 w-5" />
              <span>{apiError}</span>
            </div>
          )}

          {/* 验证结果 */}
          {validationResult && saveStatus !== 'success' && (
            <div
              className={cn(
                'flex items-start gap-2 rounded-lg border p-3',
                validationResult.is_valid
                  ? 'border-green-500/50 bg-green-500/10 text-green-500'
                  : 'border-red-500/50 bg-red-500/10 text-red-500'
              )}
            >
              {validationResult.is_valid ? (
                <>
                  <CheckCircle className="h-5 w-5 flex-shrink-0 mt-0.5" />
                  <span>代码验证通过</span>
                </>
              ) : (
                <>
                  <XCircle className="h-5 w-5 flex-shrink-0 mt-0.5" />
                  <div className="flex-1">
                    <div className="font-medium">验证失败</div>
                    {validationResult.error_message && (
                      <div className="mt-1 text-sm opacity-90">
                        {validationResult.error_message}
                      </div>
                    )}
                    {validationResult.errors && validationResult.errors.length > 0 && (
                      <ul className="mt-1 text-sm opacity-90 list-disc list-inside">
                        {validationResult.errors.map((err, i) => (
                          <li key={i}>{err}</li>
                        ))}
                      </ul>
                    )}
                  </div>
                </>
              )}
            </div>
          )}

          {/* 代码编辑器 */}
          <Card>
            <CardHeader>
              <CardTitle>策略代码</CardTitle>
            </CardHeader>
            <CardContent className="space-y-2">
              <form.Field
                name="code"
                validators={{
                  onChange: ({ value }) =>
                    !value || value.length === 0 ? '策略代码不能为空' : undefined,
                }}
              >
                {(field) => {
                  const errors = field.state.meta.errors
                  const serverError = field.state.meta.errorMap?.onServer as string | undefined
                  const hasError = errors.length > 0 || !!serverError
                  const errorMessage = serverError || getErrorMessage(errors)

                  return (
                    <>
                      <Textarea
                        value={field.state.value}
                        onChange={(e) => {
                          field.handleChange(e.target.value)
                          setValidationResult(null)
                          handleFieldChange('code')
                        }}
                        onBlur={field.handleBlur}
                        className={cn(
                          'font-mono text-sm min-h-[500px] resize-y',
                          hasError && 'border-red-500 focus-visible:ring-red-500'
                        )}
                        placeholder="编写 Backtrader 策略代码..."
                      />
                      {hasError ? (
                        <p className="text-sm text-red-500">{errorMessage}</p>
                      ) : null}
                    </>
                  )
                }}
              </form.Field>
            </CardContent>
          </Card>
        </div>

        {/* 右侧：设置 */}
        <div className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitle>基本信息</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              {/* 策略名称 */}
              <form.Field
                name="name"
                validators={{
                  onChange: ({ value }) =>
                    value.length < 3 ? '策略名称至少需要3个字符' : undefined,
                }}
              >
                {(field) => {
                  const errors = field.state.meta.errors
                  const serverError = field.state.meta.errorMap?.onServer as string | undefined
                  const hasError = errors.length > 0 || !!serverError
                  const errorMessage = serverError || getErrorMessage(errors)

                  return (
                    <div className="space-y-2">
                      <label className="text-sm font-medium">策略名称 *</label>
                      <Input
                        value={field.state.value}
                        onChange={(e) => {
                          field.handleChange(e.target.value)
                          handleFieldChange('name')
                        }}
                        onBlur={field.handleBlur}
                        placeholder="输入策略名称"
                        className={cn(
                          hasError && 'border-red-500 focus-visible:ring-red-500'
                        )}
                      />
                      {hasError ? (
                        <p className="text-sm text-red-500">{errorMessage}</p>
                      ) : null}
                    </div>
                  )
                }}
              </form.Field>

              {/* 策略描述 */}
              <form.Field name="description">
                {(field) => (
                  <div className="space-y-2">
                    <label className="text-sm font-medium">策略描述</label>
                    <Textarea
                      value={field.state.value ?? ''}
                      onChange={(e) => {
                        field.handleChange(e.target.value)
                        handleFieldChange('description')
                      }}
                      onBlur={field.handleBlur}
                      placeholder="描述策略的逻辑和特点"
                      rows={3}
                    />
                  </div>
                )}
              </form.Field>

              {/* 策略类型 */}
              <form.Field name="strategyType">
                {(field) => (
                  <div className="space-y-2">
                    <label className="text-sm font-medium">策略类型</label>
                    <select
                      className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                      value={field.state.value}
                      onChange={(e) => {
                        field.handleChange(e.target.value as StrategyFormValues['strategyType'])
                        handleFieldChange('strategyType')
                      }}
                      onBlur={field.handleBlur}
                    >
                      {strategyTypeOptions.map((type) => (
                        <option key={type.value} value={type.value}>
                          {type.label}
                        </option>
                      ))}
                    </select>
                  </div>
                )}
              </form.Field>

              {/* 指标 */}
              <form.Field name="indicatorsUsed">
                {(field) => (
                  <div className="space-y-2">
                    <label className="text-sm font-medium">使用的指标</label>
                    <div className="flex gap-2">
                      <Input
                        value={indicatorInput}
                        onChange={(e) => setIndicatorInput(e.target.value)}
                        placeholder="如: SMA, RSI, MACD"
                        onKeyDown={(e) => {
                          if (e.key === 'Enter') {
                            e.preventDefault()
                            handleAddIndicator()
                          }
                        }}
                      />
                      <Button type="button" variant="outline" onClick={handleAddIndicator}>
                        添加
                      </Button>
                    </div>
                    {field.state.value.length > 0 ? (
                      <div className="flex flex-wrap gap-2 mt-2">
                        {field.state.value.map((indicator) => (
                          <span
                            key={indicator}
                            className="flex items-center gap-1 rounded-full bg-secondary px-3 py-1 text-sm"
                          >
                            {indicator}
                            <button
                              type="button"
                              onClick={() => handleRemoveIndicator(indicator)}
                              className="ml-1 hover:text-destructive"
                            >
                              &times;
                            </button>
                          </span>
                        ))}
                      </div>
                    ) : null}
                  </div>
                )}
              </form.Field>
            </CardContent>
          </Card>

          {/* 模板卡片 */}
          <Card>
            <CardHeader>
              <CardTitle>策略模板</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-2">
                {templates?.map((template) => (
                  <div
                    key={template.name}
                    className="flex items-center justify-between rounded-lg border p-3 hover:bg-accent transition-colors"
                  >
                    <div>
                      <p className="font-medium text-sm">{template.name}</p>
                      <p className="text-xs text-muted-foreground">
                        {template.description}
                      </p>
                    </div>
                    <Button
                      type="button"
                      variant="ghost"
                      size="icon"
                      onClick={() => handleUseTemplate(template.code)}
                      title="使用此模板"
                    >
                      <Copy className="h-4 w-4" />
                    </Button>
                  </div>
                ))}
                {!templates?.length && (
                  <p className="text-sm text-muted-foreground text-center py-4">
                    暂无模板
                  </p>
                )}
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  )
}

// 主页面组件 - 处理数据加载
export default function StrategyEditorPage() {
  const { strategyId } = useParams<{ strategyId: string }>()
  const isEditing = strategyId && strategyId !== 'new'

  // 数据获取
  const { data: strategy, isLoading: isLoadingStrategy } =
    useGetStrategyApiV1StrategiesStrategyIdGet(strategyId || '', {
      query: { enabled: !!isEditing },
    })

  // 编辑模式下等待数据加载完成
  if (isEditing && isLoadingStrategy) {
    return (
      <div className="flex items-center justify-center p-12">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  // 数据加载完成后渲染表单，使用 key 确保编辑不同策略时组件重新挂载
  return (
    <StrategyEditorForm
      key={strategyId || 'new'}
      strategy={strategy}
      isEditing={!!isEditing}
    />
  )
}
