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
 * Larger, more elaborate design for centered display
 */
function ComputingAnimation({ progress }: { progress: number }) {
  const cx = 100 // center x
  const cy = 100 // center y

  return (
    <svg viewBox="0 0 200 200" className="w-[160px] h-[160px]">
      {/* Outer rotating ring */}
      <motion.g
        animate={{ rotate: 360 }}
        transition={{ duration: 20, repeat: Infinity, ease: 'linear' }}
        style={{ transformOrigin: `${cx}px ${cy}px` }}
      >
        <circle cx={cx} cy={cy} r="85" fill="none" stroke="url(#outerRingGradient)" strokeWidth="1" opacity={0.3} strokeDasharray="8 4" />
      </motion.g>

      {/* Middle counter-rotating ring */}
      <motion.g
        animate={{ rotate: -360 }}
        transition={{ duration: 15, repeat: Infinity, ease: 'linear' }}
        style={{ transformOrigin: `${cx}px ${cy}px` }}
      >
        <circle cx={cx} cy={cy} r="65" fill="none" stroke="url(#middleRingGradient)" strokeWidth="1.5" opacity={0.4} strokeDasharray="12 6" />
      </motion.g>

      {/* Progress arc (main) */}
      <motion.circle
        cx={cx}
        cy={cy}
        r="45"
        fill="none"
        stroke="url(#progressGradient)"
        strokeWidth="3"
        strokeLinecap="round"
        strokeDasharray="283"
        animate={{ strokeDashoffset: 283 - (283 * progress) / 100 }}
        transition={{ duration: 0.3 }}
        style={{ transform: 'rotate(-90deg)', transformOrigin: `${cx}px ${cy}px` }}
      />

      {/* Progress arc background */}
      <circle cx={cx} cy={cy} r="45" fill="none" stroke="currentColor" strokeWidth="3" opacity={0.1} />

      {/* Inner hexagon frame */}
      <motion.polygon
        points="100,72 124,86 124,114 100,128 76,114 76,86"
        fill="none"
        stroke="url(#hexGradient)"
        strokeWidth="1.5"
        animate={{ rotate: [0, 60] }}
        transition={{ duration: 8, repeat: Infinity, ease: 'linear' }}
        style={{ transformOrigin: `${cx}px ${cy}px` }}
        opacity={0.6}
      />

      {/* Central core */}
      <motion.circle
        cx={cx}
        cy={cy}
        r="18"
        fill="none"
        stroke="url(#coreGradient)"
        strokeWidth="2"
        animate={{ scale: [1, 1.08, 1] }}
        transition={{ duration: 2, repeat: Infinity }}
      />
      <motion.circle
        cx={cx}
        cy={cy}
        r="8"
        fill="url(#coreGradient)"
        animate={{ opacity: [0.6, 1, 0.6] }}
        transition={{ duration: 1.5, repeat: Infinity }}
      />

      {/* Orbiting satellites - outer ring */}
      <motion.g
        animate={{ rotate: 360 }}
        transition={{ duration: 6, repeat: Infinity, ease: 'linear' }}
        style={{ transformOrigin: `${cx}px ${cy}px` }}
      >
        <circle cx={cx} cy={35} r="4" fill="#3b82f6" />
        <circle cx={cx} cy={35} r="2" fill="#60a5fa" />
      </motion.g>
      <motion.g
        animate={{ rotate: -360 }}
        transition={{ duration: 8, repeat: Infinity, ease: 'linear' }}
        style={{ transformOrigin: `${cx}px ${cy}px` }}
      >
        <circle cx={165} cy={cy} r="3.5" fill="#8b5cf6" />
        <circle cx={165} cy={cy} r="1.5" fill="#a78bfa" />
      </motion.g>
      <motion.g
        animate={{ rotate: 360 }}
        transition={{ duration: 10, repeat: Infinity, ease: 'linear' }}
        style={{ transformOrigin: `${cx}px ${cy}px` }}
      >
        <circle cx={50} cy={140} r="3" fill="#ec4899" />
        <circle cx={50} cy={140} r="1.2" fill="#f472b6" />
      </motion.g>

      {/* Data flow particles - converging to center */}
      {[0, 72, 144, 216, 288].map((angle, i) => (
        <motion.circle
          key={`particle-in-${i}`}
          r="2"
          fill={['#3b82f6', '#8b5cf6', '#ec4899', '#3b82f6', '#8b5cf6'][i]}
          animate={{
            cx: [cx + 80 * Math.cos((angle * Math.PI) / 180), cx + 20 * Math.cos((angle * Math.PI) / 180)],
            cy: [cy + 80 * Math.sin((angle * Math.PI) / 180), cy + 20 * Math.sin((angle * Math.PI) / 180)],
            opacity: [0, 1, 0],
            scale: [0.5, 1, 0.5],
          }}
          transition={{ duration: 2, repeat: Infinity, ease: 'easeInOut', delay: i * 0.4 }}
        />
      ))}

      {/* Data flow particles - radiating from center */}
      {[36, 108, 180, 252, 324].map((angle, i) => (
        <motion.circle
          key={`particle-out-${i}`}
          r="1.5"
          fill={['#60a5fa', '#a78bfa', '#f472b6', '#60a5fa', '#a78bfa'][i]}
          animate={{
            cx: [cx + 25 * Math.cos((angle * Math.PI) / 180), cx + 70 * Math.cos((angle * Math.PI) / 180)],
            cy: [cy + 25 * Math.sin((angle * Math.PI) / 180), cy + 70 * Math.sin((angle * Math.PI) / 180)],
            opacity: [0, 0.8, 0],
          }}
          transition={{ duration: 2.5, repeat: Infinity, ease: 'easeOut', delay: i * 0.5 + 1 }}
        />
      ))}

      {/* Pulse rings */}
      <motion.circle
        cx={cx}
        cy={cy}
        fill="none"
        stroke="#3b82f6"
        strokeWidth="1"
        animate={{ r: [25, 60], opacity: [0.6, 0] }}
        transition={{ duration: 2.5, repeat: Infinity }}
      />
      <motion.circle
        cx={cx}
        cy={cy}
        fill="none"
        stroke="#8b5cf6"
        strokeWidth="1"
        animate={{ r: [25, 60], opacity: [0.6, 0] }}
        transition={{ duration: 2.5, repeat: Infinity, delay: 0.8 }}
      />
      <motion.circle
        cx={cx}
        cy={cy}
        fill="none"
        stroke="#ec4899"
        strokeWidth="1"
        animate={{ r: [25, 60], opacity: [0.6, 0] }}
        transition={{ duration: 2.5, repeat: Infinity, delay: 1.6 }}
      />

      {/* Corner decorative nodes */}
      {[
        { x: 25, y: 25 },
        { x: 175, y: 25 },
        { x: 25, y: 175 },
        { x: 175, y: 175 },
      ].map((pos, i) => (
        <motion.circle
          key={`corner-${i}`}
          cx={pos.x}
          cy={pos.y}
          r="3"
          fill="none"
          stroke={['#3b82f6', '#8b5cf6', '#ec4899', '#3b82f6'][i]}
          strokeWidth="1"
          animate={{ opacity: [0.3, 0.8, 0.3] }}
          transition={{ duration: 2, repeat: Infinity, delay: i * 0.5 }}
        />
      ))}

      {/* Connection lines to corners */}
      {[
        { x1: 25, y1: 25, x2: 70, y2: 70 },
        { x1: 175, y1: 25, x2: 130, y2: 70 },
        { x1: 25, y1: 175, x2: 70, y2: 130 },
        { x1: 175, y1: 175, x2: 130, y2: 130 },
      ].map((line, i) => (
        <motion.line
          key={`line-${i}`}
          x1={line.x1}
          y1={line.y1}
          x2={line.x2}
          y2={line.y2}
          stroke={['#3b82f6', '#8b5cf6', '#ec4899', '#3b82f6'][i]}
          strokeWidth="0.5"
          animate={{ opacity: [0.1, 0.4, 0.1] }}
          transition={{ duration: 3, repeat: Infinity, delay: i * 0.3 }}
        />
      ))}

      {/* Gradients */}
      <defs>
        <linearGradient id="outerRingGradient" x1="0%" y1="0%" x2="100%" y2="100%">
          <stop offset="0%" stopColor="#3b82f6" />
          <stop offset="100%" stopColor="#ec4899" />
        </linearGradient>
        <linearGradient id="middleRingGradient" x1="0%" y1="0%" x2="100%" y2="100%">
          <stop offset="0%" stopColor="#8b5cf6" />
          <stop offset="100%" stopColor="#3b82f6" />
        </linearGradient>
        <linearGradient id="progressGradient" x1="0%" y1="0%" x2="100%" y2="0%">
          <stop offset="0%" stopColor="#3b82f6" />
          <stop offset="50%" stopColor="#8b5cf6" />
          <stop offset="100%" stopColor="#ec4899" />
        </linearGradient>
        <linearGradient id="hexGradient" x1="0%" y1="0%" x2="100%" y2="100%">
          <stop offset="0%" stopColor="#3b82f6" />
          <stop offset="100%" stopColor="#8b5cf6" />
        </linearGradient>
        <linearGradient id="coreGradient" x1="0%" y1="0%" x2="100%" y2="100%">
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
 * Vertical centered layout with SVG on top
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
        'flex flex-col items-center justify-center py-8',
        className
      )}
    >
      {/* SVG Animation - centered */}
      <div className="mb-4">
        <ComputingAnimation progress={progress} />
      </div>

      {/* Title + Progress */}
      <div className="flex items-center gap-2 mb-3">
        <span className="text-sm font-medium text-foreground/80">
          {title}
        </span>
        <span className="text-xs text-muted-foreground font-mono">
          {progress.toFixed(0)}%
        </span>
      </div>

      {/* Steps list - horizontal with equal spacing */}
      <div className="flex items-center justify-center gap-6">
        {steps.map((step, idx) => (
          <StepLine key={step.id} step={step} index={idx} />
        ))}
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
