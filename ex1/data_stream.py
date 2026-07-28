from typing import Any
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._data: list[tuple[int, str]] = []
        self._next_rank: int = 0
        self._total_processed = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self._data:
            raise IndexError("No data available")

        return self._data.pop(0)

    def data_len(self) -> int:
        return len(self._data)


class NumericProcessor(DataProcessor):
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
                self._total_processed += 1

        else:
            self._data.append((self._next_rank, str(data)))
            self._next_rank += 1
            self._total_processed += 1


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
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
                self._total_processed += 1

        else:
            self._data.append((self._next_rank, data))
            self._next_rank += 1
            self._total_processed += 1


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def validate(self, data: Any) -> bool:
        if isinstance(data, list):
            logs = data

        elif isinstance(data, dict):
            logs = [data]

        else:
            return False

        return all(
            isinstance(log, dict)
            and isinstance(log.get("log_level"), str)
            and isinstance(log.get("log_message"), str)
            and all(
                isinstance(key, str) and isinstance(value, str)
                for key, value in log.items()
            )
            for log in logs
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
            self._total_processed += 1


class DataStream:
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for data in stream:
            for processor in self._processors:
                if processor.validate(data):
                    processor.ingest(data)
                    break

            else:
                print(
                    "DataStream error "
                    f"- Can't process element in stream: {data}"
                )

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")

        if self._processors == []:
            print("No processor found, no data\n")

        for processor in self._processors:
            print(
                f"{processor.__class__.__name__}: "
                f"total {processor._total_processed} items processed, "
                f"remaining {processor.data_len()} on processor"
            )


if __name__ == "__main__":
    data_stream = DataStream()
    print(
        "=== Code Nexus - Data Stream ===\n\n"
        "Initialize Data Stream..."
    )

    data_stream.print_processors_stats()

    numeric = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()

    print("Registering Numeric Processor\n")
    data_stream.register_processor(numeric)

    datas = [
        'Hello world', [3.14, -1, 2.71],
        [{'log_level': 'WARNING', 'log_message':
          'Telnet access! Use ssh instead'},
         {'log_level': 'INFO', 'log_message':
          'User wil is connected'}],
        42, ['Hi', 'five']
        ]

    print(f"Send first batch of data on stream: {datas}\n")
    data_stream.process_stream(datas)
    data_stream.print_processors_stats()
    print("")

    data_stream.register_processor(text)
    data_stream.register_processor(log)
    print("registering other data processors")

    data_stream.process_stream(datas)
    print("Send the same batch again")
    data_stream.print_processors_stats()
    print("")

    for _ in range(3):
        numeric.output()
    for _ in range(2):
        text.output()
    log.output()

    print(
        "Consume some elements from the data processors: "
        "Numeric 3, Text 2, Log 1\n"
    )
    data_stream.print_processors_stats()
