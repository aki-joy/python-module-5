from typing import Any, Protocol
from abc import ABC, abstractmethod


class DataProcessor(ABC):
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


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pass


class CSVPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        values: list[str] = []

        for rank, value in data:
            values.append(value)
        print(",".join(values))


class JSONPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        json_items: list[str] = []

        for rank, value in data:
            json_item = (
                f'"item_{rank}": "{value}"'
            )
            json_items.append(json_item)

        json_output = "{" + ", ".join(json_items) + "}"

        print(json_output)


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
        print("")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:

        for processor in self._processors:
            data: list[tuple[int, str]] = []

            for _ in range(min(nb, processor.data_len())):
                data.append(processor.output())

            print(f"{plugin.__class__.__name__} output:")
            plugin.process_output(data)


if __name__ == "__main__":
    print(
        "=== Code Nexus - Data Pipeline ===\n\n"
        "Initialize Data Stream...\n"
    )

    data_stream = DataStream()

    data_stream.print_processors_stats()

    numeric = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()

    data_stream.register_processor(numeric)
    data_stream.register_processor(text)
    data_stream.register_processor(log)

    print("Registering Processors\n")

    datas = [
        'Hello world', [3.14, -1, 2.71],
        [{'log_level': 'WARNING', 'log_message':
          'Telnet access! Use ssh instead'},
         {'log_level': 'INFO', 'log_message':
          'User wil is connected'}],
        42, ['Hi', 'five']
    ]

    data_stream.process_stream(datas)

    print(
        f"Send first batch of data on stream: {datas}\n\n"
    )

    data_stream.print_processors_stats()

    csv = CSVPlugin()

    print("Send 3 processed data from each processor to a CSV plugin:")

    data_stream.output_pipeline(3, csv)
    print("")

    data_stream.print_processors_stats()

    data2 = [
        21, ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
        [{'log_level': 'ERROR',
          'log_message': '500 server crash'},
         {'log_level': 'NOTICE',
          'log_message': 'Certificateexpires in 10 days'}],
        [32, 42, 64, 84, 128, 168], 'World hello'
    ]

    data_stream.process_stream(data2)
    print(f"Send another batch of data: {data2}\n")

    data_stream.print_processors_stats()

    json = JSONPlugin()

    print("Send 5 processed data from each processor to a JSON plugin:")

    data_stream.output_pipeline(5, json)
    print("")

    data_stream.print_processors_stats()
