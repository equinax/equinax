import { useState, useEffect } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
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
import { useQueryClient } from '@tanstack/react-query'

const strategyTypes = [
  { value: 'trend_following', label: '趋势跟踪' },
  { value: 'momentum', label: '动量策略' },
  { value: 'mean_reversion', label: '均值回归' },
  { value: 'arbitrage', label: '套利策略' },
  { value: 'other', label: '其他' },
]

const defaultCode = `class MyStrategy(bt.Strategy):
    """
    自定义策略模板

    参数:
        period: 均线周期
    """
    params = (
        ('period', 20),
    )

    def __init__(self):
        self.sma = bt.indicators.SMA(self.data.close, period=self.p.period)

    def next(self):
        if not self.position:
            if self.data.close[0] > self.sma[0]:
                self.buy()
        elif self.data.close[0] < self.sma[0]:
            self.close()
`

// Field error type
interface FieldErrors {
  name?: string
  code?: string
}

// Save status type
type SaveStatus = 'idle' | 'saving' | 'success' | 'error'

export default function StrategyEditorPage() {
  const { strategyId } = useParams<{ strategyId: string }>()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const isEditing = strategyId && strategyId !== 'new'

  // Form state
  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [strategyType, setStrategyType] = useState('trend_following')
  const [code, setCode] = useState(defaultCode)
  const [indicatorsUsed, setIndicatorsUsed] = useState<string[]>([])
  const [indicatorInput, setIndicatorInput] = useState('')

  // Validation state
  const [validationResult, setValidationResult] = useState<{
    is_valid: boolean
    error_message?: string | null
    errors?: string[]
  } | null>(null)

  // Field-level errors (from form validation or API)
  const [fieldErrors, setFieldErrors] = useState<FieldErrors>({})

  // Save status
  const [saveStatus, setSaveStatus] = useState<SaveStatus>('idle')
  const [apiError, setApiError] = useState<string | null>(null)

  // Fetch existing strategy if editing
  const { data: strategy, isLoading: isLoadingStrategy } =
    useGetStrategyApiV1StrategiesStrategyIdGet(strategyId || '', {
      query: { enabled: !!isEditing },
    })

  // Fetch templates
  const { data: templates } = useListStrategyTemplatesApiV1StrategiesTemplatesListGet()

  // Mutations
  const createMutation = useCreateStrategyApiV1StrategiesPost({
    mutation: {
      onSuccess: (data) => {
        queryClient.invalidateQueries({ queryKey: ['/api/v1/strategies'] })
        setSaveStatus('success')
        setApiError(null)
        setFieldErrors({})
        setValidationResult(null) // Clear validation result on save success
        // Navigate after showing success briefly
        setTimeout(() => {
          navigate(`/strategies/${data.id}`)
        }, 800)
      },
      onError: (error: any) => {
        setSaveStatus('error')
        handleApiError(error)
      },
    },
  })

  const updateMutation = useUpdateStrategyApiV1StrategiesStrategyIdPut({
    mutation: {
      onSuccess: () => {
        queryClient.invalidateQueries({ queryKey: ['/api/v1/strategies'] })
        queryClient.invalidateQueries({
          queryKey: [`/api/v1/strategies/${strategyId}`],
        })
        setSaveStatus('success')
        setApiError(null)
        setFieldErrors({})
        setValidationResult(null) // Clear validation result on save success
        // Reset to idle after showing success
        setTimeout(() => {
          setSaveStatus('idle')
        }, 2000)
      },
      onError: (error: any) => {
        setSaveStatus('error')
        handleApiError(error)
      },
    },
  })

  const validateMutation = useValidateCodeInlineApiV1StrategiesValidateCodePost({
    mutation: {
      onSuccess: (data) => {
        setValidationResult(data)
        // Clear save success status when re-validating
        if (saveStatus === 'success') {
          setSaveStatus('idle')
        }
      },
    },
  })

  // Handle API errors - extract field-level errors
  const handleApiError = (error: any) => {
    const newFieldErrors: FieldErrors = {}
    let generalError = '保存失败，请稍后重试'

    try {
      // Handle FastAPI validation errors (422)
      if (error?.response?.data?.detail) {
        const detail = error.response.data.detail
        if (Array.isArray(detail)) {
          detail.forEach((err: any) => {
            const field = err.loc?.[err.loc.length - 1]
            const msg = err.msg
            if (field === 'name') {
              newFieldErrors.name = msg
            } else if (field === 'code') {
              newFieldErrors.code = msg
            }
          })
          if (Object.keys(newFieldErrors).length === 0) {
            generalError = detail.map((e: any) => e.msg).join('; ')
          }
        } else if (typeof detail === 'string') {
          generalError = detail
        }
      } else if (error?.message) {
        generalError = error.message
      }
    } catch {
      // Keep default error message
    }

    setFieldErrors(newFieldErrors)
    if (Object.keys(newFieldErrors).length === 0) {
      setApiError(generalError)
    } else {
      setApiError(null)
    }
  }

  // Track if initial load has happened
  const [initialLoaded, setInitialLoaded] = useState(false)

  // Load strategy data when editing (only on initial load)
  useEffect(() => {
    if (strategy && !initialLoaded) {
      setName(strategy.name)
      setDescription(strategy.description || '')
      setStrategyType(strategy.strategy_type || 'other')
      setCode(strategy.code)
      setIndicatorsUsed(strategy.indicators_used || [])
      // Don't show validation result on load - only show when user clicks validate
      setInitialLoaded(true)
    }
  }, [strategy, initialLoaded])

  // Reset save status when form changes
  useEffect(() => {
    if (saveStatus === 'success') {
      setSaveStatus('idle')
    }
    if (saveStatus === 'error') {
      setSaveStatus('idle')
      setApiError(null)
    }
  }, [name, description, strategyType, code])

  const handleValidate = () => {
    validateMutation.mutate({ data: { code } })
  }

  // No frontend validation - rely on API validation only
  const validateForm = (): boolean => {
    setFieldErrors({})
    return true
  }

  const handleSave = () => {
    // Clear previous states
    setApiError(null)

    // Validate form
    if (!validateForm()) {
      return
    }

    setSaveStatus('saving')

    const payload = {
      name,
      description: description || null,
      strategy_type: strategyType,
      code,
      indicators_used: indicatorsUsed,
    }

    if (isEditing) {
      updateMutation.mutate({
        strategyId: strategyId!,
        data: payload,
      })
    } else {
      createMutation.mutate({ data: payload })
    }
  }

  const handleAddIndicator = () => {
    if (indicatorInput.trim() && !indicatorsUsed.includes(indicatorInput.trim())) {
      setIndicatorsUsed([...indicatorsUsed, indicatorInput.trim()])
      setIndicatorInput('')
    }
  }

  const handleRemoveIndicator = (indicator: string) => {
    setIndicatorsUsed(indicatorsUsed.filter((i) => i !== indicator))
  }

  const handleUseTemplate = (templateCode: string) => {
    setCode(templateCode)
    setValidationResult(null)
    setFieldErrors((prev) => ({ ...prev, code: undefined }))
  }

  const handleNameChange = (value: string) => {
    setName(value)
    if (fieldErrors.name) {
      setFieldErrors((prev) => ({ ...prev, name: undefined }))
    }
  }

  const handleCodeChange = (value: string) => {
    setCode(value)
    setValidationResult(null)
    if (fieldErrors.code) {
      setFieldErrors((prev) => ({ ...prev, code: undefined }))
    }
  }

  const isSaving = saveStatus === 'saving'

  if (isEditing && isLoadingStrategy) {
    return (
      <div className="flex items-center justify-center p-12">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

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
        {/* Left: Code editor */}
        <div className="lg:col-span-2 space-y-4">
          {/* Save status feedback */}
          {saveStatus === 'success' && (
            <div className="flex items-center gap-2 rounded-lg border border-green-500/50 bg-green-500/10 text-green-500 p-3">
              <CheckCircle className="h-5 w-5" />
              <span>策略保存成功</span>
            </div>
          )}

          {/* API error feedback */}
          {apiError && (
            <div className="flex items-center gap-2 rounded-lg border border-red-500/50 bg-red-500/10 text-red-500 p-3">
              <XCircle className="h-5 w-5" />
              <span>{apiError}</span>
            </div>
          )}

          {/* Validation result - only show when not just saved successfully */}
          {validationResult && saveStatus !== 'success' && (
            <div
              className={`flex items-start gap-2 rounded-lg border p-3 ${
                validationResult.is_valid
                  ? 'border-green-500/50 bg-green-500/10 text-green-500'
                  : 'border-red-500/50 bg-red-500/10 text-red-500'
              }`}
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

          {/* Code editor */}
          <Card>
            <CardHeader>
              <CardTitle>策略代码</CardTitle>
            </CardHeader>
            <CardContent className="space-y-2">
              <Textarea
                value={code}
                onChange={(e) => handleCodeChange(e.target.value)}
                className={`font-mono text-sm min-h-[500px] resize-y ${
                  fieldErrors.code ? 'border-red-500 focus-visible:ring-red-500' : ''
                }`}
                placeholder="编写 Backtrader 策略代码..."
              />
              {fieldErrors.code && (
                <p className="text-sm text-red-500">{fieldErrors.code}</p>
              )}
            </CardContent>
          </Card>
        </div>

        {/* Right: Settings & Templates */}
        <div className="space-y-4">
          {/* Basic info */}
          <Card>
            <CardHeader>
              <CardTitle>基本信息</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">策略名称 *</label>
                <Input
                  value={name}
                  onChange={(e) => handleNameChange(e.target.value)}
                  placeholder="输入策略名称"
                  className={fieldErrors.name ? 'border-red-500 focus-visible:ring-red-500' : ''}
                />
                {fieldErrors.name && (
                  <p className="text-sm text-red-500">{fieldErrors.name}</p>
                )}
              </div>

              <div className="space-y-2">
                <label className="text-sm font-medium">策略描述</label>
                <Textarea
                  value={description}
                  onChange={(e) => setDescription(e.target.value)}
                  placeholder="描述策略的逻辑和特点"
                  rows={3}
                />
              </div>

              <div className="space-y-2">
                <label className="text-sm font-medium">策略类型</label>
                <select
                  className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                  value={strategyType}
                  onChange={(e) => setStrategyType(e.target.value)}
                >
                  {strategyTypes.map((type) => (
                    <option key={type.value} value={type.value}>
                      {type.label}
                    </option>
                  ))}
                </select>
              </div>

              {/* Indicators */}
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
                  <Button variant="outline" onClick={handleAddIndicator}>
                    添加
                  </Button>
                </div>
                {indicatorsUsed.length > 0 && (
                  <div className="flex flex-wrap gap-2 mt-2">
                    {indicatorsUsed.map((indicator) => (
                      <span
                        key={indicator}
                        className="flex items-center gap-1 rounded-full bg-secondary px-3 py-1 text-sm"
                      >
                        {indicator}
                        <button
                          onClick={() => handleRemoveIndicator(indicator)}
                          className="ml-1 hover:text-destructive"
                        >
                          &times;
                        </button>
                      </span>
                    ))}
                  </div>
                )}
              </div>
            </CardContent>
          </Card>

          {/* Templates */}
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
