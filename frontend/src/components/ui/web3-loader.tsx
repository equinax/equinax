import { motion } from 'motion/react'
import { cn } from '@/lib/utils'

interface GlassSpinnerProps {
  size?: 'sm' | 'md' | 'lg'
  className?: string
}

/**
 * Glassmorphism loading spinner with gradient glow effect
 * Web3/DeFi style animated loader
 */
export function GlassSpinner({ size = 'md', className }: GlassSpinnerProps) {
  const sizes = {
    sm: { outer: 'w-6 h-6', inner: 'inset-0.5' },
    md: { outer: 'w-10 h-10', inner: 'inset-1' },
    lg: { outer: 'w-16 h-16', inner: 'inset-1.5' },
  }

  return (
    <div className={cn('relative', sizes[size].outer, className)}>
      {/* Outer rotating gradient ring */}
      <motion.div
        animate={{ rotate: 360 }}
        transition={{ duration: 1.5, repeat: Infinity, ease: 'linear' }}
        className={cn(
          'absolute inset-0 rounded-full',
          'bg-gradient-to-r from-blue-500 via-purple-500 to-pink-500',
          'blur-[2px] opacity-70'
        )}
      />
      {/* Inner glassmorphism circle */}
      <div
        className={cn(
          'absolute rounded-full',
          sizes[size].inner,
          'bg-card/60 backdrop-blur-sm border border-white/20'
        )}
      />
      {/* Center dot */}
      <motion.div
        animate={{ scale: [1, 1.2, 1] }}
        transition={{ duration: 1, repeat: Infinity, ease: 'easeInOut' }}
        className="absolute inset-0 flex items-center justify-center"
      >
        <div className="w-1.5 h-1.5 rounded-full bg-gradient-to-r from-blue-400 to-purple-400" />
      </motion.div>
    </div>
  )
}

interface ShimmerSkeletonProps {
  className?: string
}

/**
 * Shimmer skeleton loading placeholder with gradient sweep effect
 */
export function ShimmerSkeleton({ className }: ShimmerSkeletonProps) {
  return (
    <div className={cn('relative overflow-hidden bg-muted rounded-md', className)}>
      <motion.div
        animate={{ x: ['-100%', '100%'] }}
        transition={{
          duration: 1.5,
          repeat: Infinity,
          ease: 'linear',
        }}
        className="absolute inset-0 bg-gradient-to-r from-transparent via-white/10 to-transparent"
      />
    </div>
  )
}

interface LoadingOverlayProps {
  children?: React.ReactNode
  className?: string
}

/**
 * Full area loading overlay with glassmorphism backdrop
 */
export function LoadingOverlay({ children, className }: LoadingOverlayProps) {
  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      className={cn(
        'absolute inset-0 flex items-center justify-center',
        'bg-background/50 backdrop-blur-sm z-10',
        className
      )}
    >
      {children ?? <GlassSpinner size="lg" />}
    </motion.div>
  )
}

// Animation variants for staggered reveals
export const staggerContainer = {
  hidden: { opacity: 0 },
  show: {
    opacity: 1,
    transition: {
      staggerChildren: 0.05,
    },
  },
}

export const staggerItem = {
  hidden: { opacity: 0, y: 10 },
  show: {
    opacity: 1,
    y: 0,
    transition: {
      duration: 0.3,
      ease: 'easeOut' as const,
    },
  },
}

// Fade in animation for content
export const fadeIn = {
  initial: { opacity: 0 },
  animate: { opacity: 1 },
  exit: { opacity: 0 },
  transition: { duration: 0.2 },
}
