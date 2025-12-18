import { Routes, Route } from 'react-router-dom'
import { ThemeProvider } from '@/components/theme-provider'
import AppLayout from '@/components/layout/AppLayout'
import DashboardPage from '@/pages/DashboardPage'
import StrategiesPage from '@/pages/StrategiesPage'
import StrategyEditorPage from '@/pages/StrategyEditorPage'
import BacktestPage from '@/pages/BacktestPage'
import ResultsPage from '@/pages/ResultsPage'
import ResultDetailPage from '@/pages/ResultDetailPage'
import TechnicalAnalysisPage from '@/pages/TechnicalAnalysisPage'
import UniverseCockpitPage from '@/pages/UniverseCockpitPage'
import SettingsPage from '@/pages/SettingsPage'

function App() {
  return (
    <ThemeProvider defaultTheme="dark" storageKey="quant-ui-theme">
      <Routes>
        <Route path="/" element={<AppLayout />}>
          <Route index element={<DashboardPage />} />
          <Route path="strategies" element={<StrategiesPage />} />
          <Route path="strategies/new" element={<StrategyEditorPage />} />
          <Route path="strategies/:strategyId" element={<StrategyEditorPage />} />
          <Route path="backtest" element={<BacktestPage />} />
          <Route path="results" element={<ResultsPage />} />
          <Route path="results/:jobId" element={<ResultDetailPage />} />
          <Route path="analysis/:jobId/:resultId" element={<TechnicalAnalysisPage />} />
          <Route path="universe" element={<UniverseCockpitPage />} />
          <Route path="settings" element={<SettingsPage />} />
        </Route>
      </Routes>
    </ThemeProvider>
  )
}

export default App
