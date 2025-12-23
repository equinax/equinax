"""
并发下载工具

提供异步并发下载能力，适用于大批量数据获取。
"""

import asyncio
import time
from typing import List, Callable, Any, Dict, TypeVar, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from functools import partial
import pandas as pd

T = TypeVar('T')


@dataclass
class DownloadResult:
    """下载结果"""
    code: str
    success: bool
    data: Any = None
    error: str = None
    elapsed: float = 0.0


class ConcurrentDownloader:
    """
    并发下载器

    支持线程池并发和异步并发两种模式。

    Usage:
        downloader = ConcurrentDownloader(max_workers=10, delay=0.05)

        # 使用线程池
        results = downloader.download_batch(
            codes=['600000', '000001'],
            download_func=lambda code: fetch_data(code)
        )

        # 使用异步
        results = await downloader.download_batch_async(
            codes=['600000', '000001'],
            download_func=async_fetch_data
        )
    """

    def __init__(
        self,
        max_workers: int = 10,
        delay: float = 0.05,
        retry_times: int = 3,
        retry_delay: float = 1.0
    ):
        """
        初始化并发下载器

        Args:
            max_workers: 最大并发数
            delay: 请求间隔（秒）
            retry_times: 重试次数
            retry_delay: 重试间隔（秒）
        """
        self.max_workers = max_workers
        self.delay = delay
        self.retry_times = retry_times
        self.retry_delay = retry_delay

    def download_batch(
        self,
        codes: List[str],
        download_func: Callable[[str], Any],
        progress_callback: Callable[[int, int], None] = None
    ) -> List[DownloadResult]:
        """
        使用线程池并发下载

        Args:
            codes: 代码列表
            download_func: 下载函数，接收 code 返回数据
            progress_callback: 进度回调 (completed, total)

        Returns:
            List[DownloadResult]
        """
        results = []
        total = len(codes)
        completed = 0

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有任务
            future_to_code = {}
            for i, code in enumerate(codes):
                # 添加延迟避免请求过快
                if i > 0 and self.delay > 0:
                    time.sleep(self.delay)

                future = executor.submit(self._download_with_retry, code, download_func)
                future_to_code[future] = code

            # 收集结果
            for future in as_completed(future_to_code):
                code = future_to_code[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    results.append(DownloadResult(
                        code=code,
                        success=False,
                        error=str(e)
                    ))

                completed += 1
                if progress_callback:
                    progress_callback(completed, total)

        return results

    def _download_with_retry(
        self,
        code: str,
        download_func: Callable[[str], Any]
    ) -> DownloadResult:
        """带重试的下载"""
        last_error = None

        for attempt in range(self.retry_times):
            try:
                start_time = time.time()
                data = download_func(code)
                elapsed = time.time() - start_time

                return DownloadResult(
                    code=code,
                    success=True,
                    data=data,
                    elapsed=elapsed
                )

            except Exception as e:
                last_error = str(e)
                if attempt < self.retry_times - 1:
                    time.sleep(self.retry_delay)

        return DownloadResult(
            code=code,
            success=False,
            error=last_error
        )

    async def download_batch_async(
        self,
        codes: List[str],
        download_func: Callable[[str], Any],
        progress_callback: Callable[[int, int], None] = None
    ) -> List[DownloadResult]:
        """
        使用异步并发下载

        Args:
            codes: 代码列表
            download_func: 异步下载函数
            progress_callback: 进度回调

        Returns:
            List[DownloadResult]
        """
        semaphore = asyncio.Semaphore(self.max_workers)
        results = []
        total = len(codes)
        completed = [0]  # 使用列表以便在闭包中修改

        async def download_one(code: str) -> DownloadResult:
            async with semaphore:
                result = await self._download_with_retry_async(code, download_func)
                completed[0] += 1
                if progress_callback:
                    progress_callback(completed[0], total)
                await asyncio.sleep(self.delay)
                return result

        tasks = [download_one(code) for code in codes]
        results = await asyncio.gather(*tasks)

        return list(results)

    async def _download_with_retry_async(
        self,
        code: str,
        download_func: Callable[[str], Any]
    ) -> DownloadResult:
        """带重试的异步下载"""
        last_error = None

        for attempt in range(self.retry_times):
            try:
                start_time = time.time()

                # 判断是否是协程函数
                if asyncio.iscoroutinefunction(download_func):
                    data = await download_func(code)
                else:
                    # 在线程池中运行同步函数
                    loop = asyncio.get_event_loop()
                    data = await loop.run_in_executor(None, download_func, code)

                elapsed = time.time() - start_time

                return DownloadResult(
                    code=code,
                    success=True,
                    data=data,
                    elapsed=elapsed
                )

            except Exception as e:
                last_error = str(e)
                if attempt < self.retry_times - 1:
                    await asyncio.sleep(self.retry_delay)

        return DownloadResult(
            code=code,
            success=False,
            error=last_error
        )


def download_batch_sync(
    codes: List[str],
    download_func: Callable[[str], pd.DataFrame],
    max_workers: int = 10,
    delay: float = 0.05,
    progress_interval: int = 100
) -> Dict[str, pd.DataFrame]:
    """
    同步批量下载（便捷函数）

    Args:
        codes: 代码列表
        download_func: 下载函数
        max_workers: 最大并发数
        delay: 请求间隔
        progress_interval: 进度提示间隔

    Returns:
        Dict[code -> DataFrame]
    """
    downloader = ConcurrentDownloader(
        max_workers=max_workers,
        delay=delay
    )

    def progress(completed, total):
        if completed % progress_interval == 0 or completed == total:
            print(f"  进度: {completed}/{total} ({completed * 100 // total}%)")

    results = downloader.download_batch(
        codes=codes,
        download_func=download_func,
        progress_callback=progress
    )

    # 转换为字典
    data_dict = {}
    success_count = 0
    fail_count = 0

    for result in results:
        if result.success and result.data is not None:
            if isinstance(result.data, pd.DataFrame) and not result.data.empty:
                data_dict[result.code] = result.data
                success_count += 1
            elif result.data:
                data_dict[result.code] = result.data
                success_count += 1
        else:
            fail_count += 1

    print(f"  完成: 成功 {success_count}, 失败 {fail_count}")

    return data_dict


async def download_batch_async(
    codes: List[str],
    download_func: Callable[[str], Any],
    max_workers: int = 10,
    delay: float = 0.05
) -> Dict[str, Any]:
    """
    异步批量下载（便捷函数）

    Args:
        codes: 代码列表
        download_func: 下载函数（同步或异步）
        max_workers: 最大并发数
        delay: 请求间隔

    Returns:
        Dict[code -> data]
    """
    downloader = ConcurrentDownloader(
        max_workers=max_workers,
        delay=delay
    )

    results = await downloader.download_batch_async(
        codes=codes,
        download_func=download_func
    )

    return {r.code: r.data for r in results if r.success and r.data is not None}


class ProgressTracker:
    """
    进度跟踪器

    提供更详细的进度信息和 ETA 估算。
    """

    def __init__(self, total: int, description: str = "下载"):
        self.total = total
        self.description = description
        self.completed = 0
        self.start_time = time.time()
        self.last_print_time = 0

    def update(self, n: int = 1):
        """更新进度"""
        self.completed += n
        self._print_progress()

    def _print_progress(self):
        """打印进度"""
        now = time.time()
        # 每秒最多打印一次
        if now - self.last_print_time < 1 and self.completed < self.total:
            return

        self.last_print_time = now
        elapsed = now - self.start_time

        if self.completed > 0:
            eta = (elapsed / self.completed) * (self.total - self.completed)
            eta_str = f"{int(eta)}s" if eta < 60 else f"{int(eta // 60)}m{int(eta % 60)}s"
        else:
            eta_str = "计算中..."

        percent = self.completed * 100 // self.total
        bar_len = 30
        filled = int(bar_len * self.completed / self.total)
        bar = '█' * filled + '░' * (bar_len - filled)

        print(f"\r  {self.description}: [{bar}] {percent}% ({self.completed}/{self.total}) ETA: {eta_str}",
              end='', flush=True)

        if self.completed >= self.total:
            print()  # 换行

    def finish(self):
        """完成"""
        elapsed = time.time() - self.start_time
        print(f"  {self.description} 完成，耗时 {elapsed:.1f}s")
