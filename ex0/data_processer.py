from typing import Any
from abc import ABC, abstractmethod


class DataProcesser(ABC):
    def __init__(self) -> None:
        self._data: list[tuple[int, str]] = []
        self._next_rank: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        return self._data[self._next_rank]


class NumericProcesser(DataProcesser):
    def __init__(self) -> None:
        super.__init__()

    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True

        if isinstance(data, list):
            return all(
                isinstance(item, (int, float))
                for item in data
            )

        return False

    def ingest(
            self,
            data: int | float | list[int | float]
    ) -> None:

        if not self.validate(data):
            raise ValueError("Improper numeric data")

        if isinstance(data, list):
            for item in data:
                self._data.append(self._next_rank, (str(item)))
                self._next_rank += 1

        else:
            self._data.append(self._next_rank, str(data))
            self._next_rank += 1


class TextProcesser(DataProcesser):
    def __init__(self):
        super().__init__()

    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True

        if isinstance(data, list):
            return all(
                isinstance(item, str)
                for item in data
            )

        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")

        if isinstance(data, list):
            for item in data:
                self._data.append((self._next_rank, item))
                self._next_rank += 1

        else:
            self._data.append(self._next_rank, item)
            self._next_rank += 1


class LogProcesser(DataProcesser):
    def __init__(self):
        super().__init__()

    def validate(self, data: dict[str, str] | list[dict[str, str]]) -> bool:
        if isinstance(data, list):
            logs = data

        else:
            logs = [data]

        for log in logs:
            return all(
                isinstance(key, str)
                for key in log.keys()
            ) and all(
                isinstance(value, str)
                for value in log.values()
            )

        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")

        if isinstance(data, list):
            logs = data
        else:
            logs = [data]
        
        for log in logs:
            processed_log = (
                log["log_level"]
                + ": "
                + log["log_message"]
            )
            self._data.append(
                (self._next_rank, processed_log)
            )
            self._next_rank += 1


if __name__ == "__main__":
    fortytwo = NumericProcesser()
    print(
        "=== Code Nexus - Data Processer ===\n"
        "Testing Numeric Processer..."
    )