import React, { useState, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { 
  Search, 
  ArrowRight, 
  ListChecks, 
  Calendar, 
  LayoutGrid, 
  Tag,
  AlertCircle
} from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Textarea } from '@/components/ui/textarea';
import { Card, CardHeader, CardTitle, CardContent, CardDescription, CardFooter } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';

const TAB_LABELS: Record<string, string> = {
  weekly: '周度精选',
  rally: '反弹潜力',
  dragon: '龙头涨停',
  overnight: '隔夜套利'
};

export default function MultiBrowseEntryPage() {
  const navigate = useNavigate();
  const [input, setInput] = useState('');

  const parsedState = useMemo(() => {
    if (!input.trim()) return null;

    let params: URLSearchParams;
    try {
      // Try parsing as a full URL first
      const url = new URL(input.trim());
      params = url.searchParams;
    } catch {
      // If not a valid URL, try to handle as query string or relative path
      const qIndex = input.indexOf('?');
      if (qIndex !== -1) {
        params = new URLSearchParams(input.slice(qIndex + 1));
      } else {
        params = new URLSearchParams(input);
      }
    }

    const codesStr = params.get('codes') || '';
    const codes = codesStr.split(',').map(c => c.trim()).filter(Boolean);
    const date = params.get('date');
    const tab = params.get('tab');
    const labels = params.get('labels');

    return {
      isValid: codes.length > 0,
      codes,
      date,
      tab,
      labels,
      rawParams: params.toString()
    };
  }, [input]);

  const handleNavigate = () => {
    if (parsedState?.isValid) {
      navigate(`/alpha-radar/multi-browse?${parsedState.rawParams}`);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleNavigate();
    }
  };

  return (
    <div className="min-h-screen bg-background flex items-center justify-center p-4">
      <Card className="w-full max-w-2xl shadow-lg border-border/50 bg-card/50 backdrop-blur-sm">
        <CardHeader className="space-y-1">
          <div className="flex items-center space-x-2">
            <div className="p-2 bg-primary/10 rounded-lg">
              <ListChecks className="w-6 h-6 text-primary" />
            </div>
            <CardTitle className="text-2xl font-bold tracking-tight">多股评估</CardTitle>
          </div>
          <CardDescription className="text-muted-foreground text-base ml-12">
            粘贴报告中的参数字符串或链接，快速恢复多股浏览页面
          </CardDescription>
        </CardHeader>
        
        <CardContent className="space-y-6">
          <div className="space-y-2">
            <Textarea
              placeholder="例如: codes=sz.300045,sh.600435&date=2025-12-29&tab=dragon"
              className="min-h-[120px] font-mono text-sm resize-none bg-background/50 focus:bg-background transition-colors"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={handleKeyDown}
            />
            <p className="text-xs text-muted-foreground flex items-center gap-1">
              <AlertCircle className="w-3 h-3" />
              支持完整 URL 或仅参数部分
            </p>
          </div>

          {parsedState && (
            <div className={`rounded-lg border p-4 transition-all duration-300 ${
              parsedState.isValid 
                ? 'bg-primary/5 border-primary/20' 
                : 'bg-destructive/5 border-destructive/20'
            }`}>
              {parsedState.isValid ? (
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                  <div className="space-y-1">
                    <span className="text-xs text-muted-foreground flex items-center gap-1">
                      <ListChecks className="w-3 h-3" /> 股票数量
                    </span>
                    <p className="text-lg font-semibold text-foreground">
                      {parsedState.codes.length} <span className="text-xs font-normal text-muted-foreground">只</span>
                    </p>
                  </div>
                  
                  <div className="space-y-1">
                    <span className="text-xs text-muted-foreground flex items-center gap-1">
                      <Calendar className="w-3 h-3" /> 日期
                    </span>
                    <p className="text-lg font-semibold text-foreground">
                      {parsedState.date || <span className="text-muted-foreground/50">-</span>}
                    </p>
                  </div>

                  <div className="space-y-1">
                    <span className="text-xs text-muted-foreground flex items-center gap-1">
                      <LayoutGrid className="w-3 h-3" /> 策略标签
                    </span>
                    <div>
                      {parsedState.tab ? (
                        <Badge variant="secondary" className="font-normal">
                          {TAB_LABELS[parsedState.tab] || parsedState.tab}
                        </Badge>
                      ) : (
                        <span className="text-muted-foreground/50">-</span>
                      )}
                    </div>
                  </div>

                  <div className="space-y-1">
                    <span className="text-xs text-muted-foreground flex items-center gap-1">
                      <Tag className="w-3 h-3" /> 筛选标签
                    </span>
                    <div>
                      {parsedState.labels ? (
                        <Badge variant="outline" className="font-normal truncate max-w-full block">
                          {parsedState.labels}
                        </Badge>
                      ) : (
                        <span className="text-muted-foreground/50">-</span>
                      )}
                    </div>
                  </div>
                </div>
              ) : (
                <div className="flex items-center gap-2 text-destructive text-sm">
                  <AlertCircle className="w-4 h-4" />
                  <span>未检测到有效的股票代码 (codes)</span>
                </div>
              )}
            </div>
          )}
        </CardContent>

        <CardFooter>
          <Button 
            className="w-full h-12 text-base font-medium shadow-lg hover:shadow-xl transition-all" 
            size="lg"
            disabled={!parsedState?.isValid}
            onClick={handleNavigate}
          >
            <Search className="w-4 h-4 mr-2" />
            进入浏览
            <ArrowRight className="w-4 h-4 ml-2 opacity-50" />
          </Button>
        </CardFooter>
      </Card>
    </div>
  );
}
