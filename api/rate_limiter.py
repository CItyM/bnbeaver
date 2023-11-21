import asyncio
from time import time
from typing import Any, List


class RateLimiter:
    def __init__(self, rate_limit: int, time_window: int) -> None:
        self.rate_limit = rate_limit
        self.time_window = time_window
        self.current_weight = 0
        self.last_execution_time = time()
        self.tasks = []
        self.results = []

    async def add_task(self, task, weight):
        current_time = time()

        if (
            self.current_weight + weight > self.rate_limit
            or current_time - self.last_execution_time >= self.time_window
        ):
            await self.execute_tasks()
            self.last_execution_time = current_time
            self.current_weight = 0

        self.tasks.append(task)
        self.current_weight += weight

    async def execute_tasks(self):
        if self.tasks:
            responses = await asyncio.gather(*self.tasks)
            self.results.extend(responses)
            self.tasks = []

    def get_results(self) -> List[Any]:
        results = self.results
        self.results = []
        return results
