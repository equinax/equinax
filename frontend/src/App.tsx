import { Routes, Route } from 'react-router-dom'
import { ThemeProvider } from '@/components/theme-provider'
import { Toaster } from '@/components/ui/toaster'
import AppLayout from '@/components/layout/AppLayout'
import DashboardPage from '@/pages/DashboardPage'
import StrategiesPage from '@/pages/StrategiesPage'
import BacktestPage from '@/pages/BacktestPage'
import ResultsPage from '@/pages/ResultsPage'
import DataExplorerPage from '@/pages/DataExplorerPage'

function App() {
  return (
    <ThemeProvider defaultTheme="dark" storageKey="quant-ui-theme">
      <Routes>
        <Route path="/" element={<AppLayout />}>
          <Route index element={<DashboardPage />} />
          <Route path="strategies" element={<StrategiesPage />} />
          <Route path="backtest" element={<BacktestPage />} />
          <Route path="results" element={<ResultsPage />} />
          <Route path="data" element={<DataExplorerPage />} />
        </Route>
      </Routes>
      <Toaster />
    </ThemeProvider>
  )
}

export default App
