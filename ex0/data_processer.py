from typing import Any
from abc import ABC, abstractmethod


class DataProcesser(ABC):
    def __init__(self) -> None:
        self._data: list[tuple[int, str]] = []
        self._next_rank: int = 0
        self.index_output = -1

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        extracted_data = self._data[0]
        del self._data[0]
        return extracted_data


class NumericProcesser(DataProcesser):
    def __init__(self) -> None:
        super().__init__()

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
                self._data.append((self._next_rank, (str(item))))
                self._next_rank += 1

        else:
            self._data.append((self._next_rank, str(data)))
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
            self._data.append((self._next_rank, data))
            self._next_rank += 1


class LogProcesser(DataProcesser):
    def __init__(self):
        super().__init__()

    def validate(self, data: dict[str, str] | list[dict[str, str]]) -> bool:
        if isinstance(data, list):
            logs = data

        elif isinstance(data, dict):
            logs = [data]

        else:
            return False

        for log in logs:
            return all(
                isinstance(key, str)
                for key in log.keys()
            ) and all(
                isinstance(value, str)
                for value in log.values()
            )

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
    numeric = NumericProcesser()
    print(
        "=== Code Nexus - Data Processer ===\n\n"
        "Testing Numeric Processer...\n"
        f" Trying to validate input '42': {numeric.validate(42)}\n"
        f" Trying to validata input 'hello': {numeric.validate('hello')}"
    )
    print(" Test invalid ingestion of string 'foo' without prior validation:")
    try:
        numeric.ingest('foo')
    except ValueError as e:
        print(f"Got exception: {e}")

    numerics = [1, 2, 3, 4, 5]
    numeric.ingest(numerics)
    print(
        f" Processing data: {numerics}\n"
        " Extracting 3 values..."
    )
    for _ in range(3):
        rank, value = numeric.output()
        print(f" Numeric value {rank}: {value}")
    print("\n")
    text = TextProcesser()
    print(
        "Testing Text Processer...\n"
        f" Trying to validate input '42': {text.validate(42)}"
    )

    texts = ["Hello", "Nexus", "World"]
    text.ingest(texts)
    print(
        f" Processing data: {texts}\n"
        " Extracting 1 value..."
    )
    rank, value = text.output()
    print(f" Text value {rank}: {value}\n\n")

    log = LogProcesser()
    print(
        "Testing Log Processer...\n"
        f" Trying to validate input 'hello': {log.validate('hello')}"
    )

    logs = [{'log_level': 'NOTICE', 'log_message': 'Connection to server'},
            {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}]
    log.ingest(logs)
    print(
        f" Processing data: {logs}\n"
        " Extracting 2 values..."
    )
    for _ in range(2):
        rank, value = log.output()
        print(f" Log entry {rank}: {value}")
