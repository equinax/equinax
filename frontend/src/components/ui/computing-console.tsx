import { motion } from 'motion/react'
import { Check, Loader2, Circle } from 'lucide-react'
import { cn } from '@/lib/utils'

export interface ComputingStep {
  id: string
  label: string
  status: 'pending' | 'running' | 'completed'
}

interface ComputingConsoleProps {
  title?: string
  steps: ComputingStep[]
  progress?: number
  className?: string
}

/**
 * Animated SVG computing visualization with multiple moving elements
 */
function ComputingAnimation({ progress }: { progress: number }) {
  return (
    <svg viewBox="0 0 120 80" className="w-[120px] h-[80px]">
      {/* Central processing node */}
      <motion.circle
        cx="60"
        cy="40"
        r="8"
        fill="none"
        stroke="url(#centerGradient)"
        strokeWidth="2"
        animate={{ scale: [1, 1.1, 1] }}
        transition={{ duration: 1.5, repeat: Infinity }}
      />
      <motion.circle
        cx="60"
        cy="40"
        r="3"
        fill="url(#centerGradient)"
        animate={{ opacity: [0.5, 1, 0.5] }}
        transition={{ duration: 1, repeat: Infinity }}
      />

      {/* Orbiting dots */}
      <motion.g
        animate={{ rotate: 360 }}
        transition={{ duration: 3, repeat: Infinity, ease: 'linear' }}
        style={{ transformOrigin: '60px 40px' }}
      >
        <circle cx="60" cy="22" r="2.5" fill="#3b82f6" />
      </motion.g>
      <motion.g
        animate={{ rotate: -360 }}
        transition={{ duration: 4, repeat: Infinity, ease: 'linear' }}
        style={{ transformOrigin: '60px 40px' }}
      >
        <circle cx="78" cy="40" r="2" fill="#8b5cf6" />
      </motion.g>
      <motion.g
        animate={{ rotate: 360 }}
        transition={{ duration: 5, repeat: Infinity, ease: 'linear' }}
        style={{ transformOrigin: '60px 40px' }}
      >
        <circle cx="45" cy="52" r="1.5" fill="#ec4899" />
      </motion.g>

      {/* Data flow particles - left to center */}
      <motion.circle
        r="1.5"
        fill="#3b82f6"
        animate={{ cx: [10, 52], cy: [40, 40], opacity: [0, 1, 0] }}
        transition={{ duration: 1.5, repeat: Infinity, ease: 'easeInOut' }}
      />
      <motion.circle
        r="1.5"
        fill="#8b5cf6"
        animate={{ cx: [10, 52], cy: [30, 38], opacity: [0, 1, 0] }}
        transition={{ duration: 1.5, repeat: Infinity, ease: 'easeInOut', delay: 0.3 }}
      />
      <motion.circle
        r="1.5"
        fill="#ec4899"
        animate={{ cx: [10, 52], cy: [50, 42], opacity: [0, 1, 0] }}
        transition={{ duration: 1.5, repeat: Infinity, ease: 'easeInOut', delay: 0.6 }}
      />

      {/* Data flow particles - center to right */}
      <motion.circle
        r="1.5"
        fill="#3b82f6"
        animate={{ cx: [68, 110], cy: [40, 40], opacity: [0, 1, 0] }}
        transition={{ duration: 1.5, repeat: Infinity, ease: 'easeInOut', delay: 0.8 }}
      />
      <motion.circle
        r="1.5"
        fill="#8b5cf6"
        animate={{ cx: [68, 110], cy: [38, 30], opacity: [0, 1, 0] }}
        transition={{ duration: 1.5, repeat: Infinity, ease: 'easeInOut', delay: 1.1 }}
      />

      {/* Progress arc */}
      <motion.path
        d="M 60 15 A 25 25 0 1 1 59.99 15"
        fill="none"
        stroke="url(#arcGradient)"
        strokeWidth="2"
        strokeLinecap="round"
        strokeDasharray="157"
        animate={{ strokeDashoffset: 157 - (157 * progress) / 100 }}
        transition={{ duration: 0.3 }}
        opacity={0.6}
      />

      {/* Pulse rings */}
      <motion.circle
        cx="60"
        cy="40"
        r="20"
        fill="none"
        stroke="#3b82f6"
        strokeWidth="0.5"
        animate={{ r: [20, 30], opacity: [0.5, 0] }}
        transition={{ duration: 2, repeat: Infinity }}
      />
      <motion.circle
        cx="60"
        cy="40"
        r="20"
        fill="none"
        stroke="#8b5cf6"
        strokeWidth="0.5"
        animate={{ r: [20, 30], opacity: [0.5, 0] }}
        transition={{ duration: 2, repeat: Infinity, delay: 1 }}
      />

      {/* Gradients */}
      <defs>
        <linearGradient id="centerGradient" x1="0%" y1="0%" x2="100%" y2="100%">
          <stop offset="0%" stopColor="#3b82f6" />
          <stop offset="100%" stopColor="#8b5cf6" />
        </linearGradient>
        <linearGradient id="arcGradient" x1="0%" y1="0%" x2="100%" y2="0%">
          <stop offset="0%" stopColor="#3b82f6" />
          <stop offset="50%" stopColor="#8b5cf6" />
          <stop offset="100%" stopColor="#ec4899" />
        </linearGradient>
      </defs>
    </svg>
  )
}

/**
 * Glassmorphism computing console with step progress
 * Shows backend computation steps with animated progress
 */
export function ComputingConsole({
  title = '正在计算...',
  steps,
  progress = 0,
  className,
}: ComputingConsoleProps) {
  return (
    <div
      className={cn(
        'relative overflow-hidden rounded-lg',
        'bg-card/80 backdrop-blur-md',
        'border border-border/50',
        className
      )}
    >
      {/* Content */}
      <div className="relative p-4 flex items-start gap-4">
        {/* Left: SVG Animation */}
        <div className="shrink-0">
          <ComputingAnimation progress={progress} />
        </div>

        {/* Right: Steps */}
        <div className="flex-1 min-w-0">
          {/* Title */}
          <div className="flex items-center gap-2 mb-2">
            <span className="text-sm font-medium text-foreground/80">
              {title}
            </span>
            <span className="text-[10px] text-muted-foreground font-mono">
              {progress.toFixed(0)}%
            </span>
          </div>

          {/* Steps list */}
          <div className="space-y-1">
            {steps.map((step, idx) => (
              <StepLine key={step.id} step={step} index={idx} />
            ))}
          </div>
        </div>
      </div>
    </div>
  )
}

function StepLine({ step, index }: { step: ComputingStep; index: number }) {
  return (
    <motion.div
      initial={{ opacity: 0, x: -10 }}
      animate={{ opacity: 1, x: 0 }}
      transition={{ delay: index * 0.08, duration: 0.2 }}
      className="flex items-center gap-2 text-xs"
    >
      {/* Status icon */}
      {step.status === 'completed' && (
        <Check className="h-3.5 w-3.5 text-green-500" />
      )}
      {step.status === 'running' && (
        <Loader2 className="h-3.5 w-3.5 text-blue-500 animate-spin" />
      )}
      {step.status === 'pending' && (
        <Circle className="h-3.5 w-3.5 text-muted-foreground/40" />
      )}

      {/* Label */}
      <span
        className={cn(
          'transition-colors duration-200',
          step.status === 'completed' && 'text-muted-foreground',
          step.status === 'running' && 'text-foreground',
          step.status === 'pending' && 'text-muted-foreground/50'
        )}
      >
        {step.label}
        {step.status === 'running' && (
          <motion.span
            animate={{ opacity: [1, 0] }}
            transition={{ duration: 0.5, repeat: Infinity }}
            className="text-blue-500"
          >
            _
          </motion.span>
        )}
      </span>
    </motion.div>
  )
}
