from typing import Any
from abc import ABC, abstractmethod


class DataProcesser(ABC):
    def __init__(self) -> None:
        self._data: list[tuple[int, str]] = []
        self._next_rank: int = 0
        self._index_output = -1
        self._total_processed = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        self._index_output += 1
        return self._data[self._index_output]

    def data_len(self) -> int:
        return len(self._data) - self._index_output - 1


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
                self._total_processed += 1

        else:
            self._data.append((self._next_rank, str(data)))
            self._next_rank += 1
            self._total_processed += 1


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
                self._total_processed += 1

        else:
            self._data.append((self._next_rank, data))
            self._next_rank += 1
            self._total_processed += 1


class LogProcesser(DataProcesser):
    def __init__(self):
        super().__init__()

    def validate(self, data: Any) -> bool:
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
            self._total_processed += 1


class DataStream:
    def __init__(self) -> None:
        self._processers: list[DataProcesser] = []

    def register_processer(self, proc: DataProcesser) -> None:
        self._processers.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for data in stream:
            for processer in self._processers:
                if processer.validate(data):
                    processer.ingest(data)
                    break

            else:
                print(
                    "DataStream error "
                    f"- Can't process element in stream: {data}"
                )

    def print_processer_stats(self) -> None:
        for processer in self._processers:
            print(
                f"{processer.__class__.__name__}: "
                f"total {processer._total_processed} items processed, "
                f"remaining {processer.data_len()} on processer"
            )


if __name__ == "__main__":
    data_stream = DataStream()
    print(
        "=== Code Nexus - Data Stream ===\n\n"
        "Initialize Data Stream..."
    )
    if data_stream._processers == []:
        print("No processer found, no data\n")

    numeric = NumericProcesser()
    text = TextProcesser()
    log = LogProcesser()

    print("Registering Numeric Processer\n")
    data_stream.register_processer(numeric)

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
    print("== DataStream statistics ==")
    data_stream.print_processer_stats()
    print("")

    data_stream.register_processer(text)
    data_stream.register_processer(log)
    print("registering other data processers")

    data_stream.process_stream(datas)
    print("Send the same batch again")
    print("== DataStream statistics ==")
    data_stream.print_processer_stats()
    print("")

    for _ in range(3):
        numeric.output()
    for _ in range(2):
        text.output()
    log.output()

    print(
        "Consume some elements from the data processer: "
        "Numeric 3, Text 2, Log 1\n"
        "== DataStream statistics =="
    )
    data_stream.print_processer_stats()
