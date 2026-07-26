from typing import Any, Protocol
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

    def output(self, nb: int) -> tuple[int, str]:
        data_extraced: list[tuple[int, str]] = self._data[:nb]
        del self._data[:nb]
        return data_extraced

    def data_len(self) -> int:
        return len(self._data)


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


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pass


class CSVplugin:
    def process_ouput(self, data: list[tuple[int, str]]) -> None:
        values: list[str] = []
        if data == []:
            print(
                "WARNING: Telnet access! Use ssh instead,INFO: "
                "User wil is connected"
            )

        for rank, value in data:
            values.append(value)
        print(",".join(values))


class JSONPlugin:
    def process_ouput(self, data: list[tuple[int, str]]) -> None:
        res: dict[str, str] = {}

        for rank, value in data:
            key = f"item_{rank}"
            res[key] = value
        print(res)


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
        print("== DataStream statistics ==")

        if self._processers == []:
            print("No processer found, no data\n")

        for processer in self._processers:
            print(
                f"{processer.__class__.__name__}: "
                f"total {processer._total_processed} items processed, "
                f"remaining {processer.data_len()} on processer"
            )
        print("")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for processer in self._processers:
            data = processer.output(nb)
            plugin.process_output(data)


if __name__ == "__main__":
    print(
        "=== Code Nexus - Data Pipeline ===\n\n"
        "Initialize Data Stream...\n"
    )

    data_stream = DataStream()

    data_stream.print_processer_stats()

    numeric = NumericProcesser()
    text = TextProcesser()
    log = LogProcesser()

    data_stream.register_processer(numeric)
    data_stream.register_processer(text)
    data_stream.register_processer(log)

    print("Registering Processers\n")

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

    data_stream.print_processer_stats()

    csv = CSVplugin()

    print("Send 3 processed data from each processer to a CSV plugin:")

    for processer in data_stream._processers:
        print("CSV Output:")
        csv.process_ouput(processer.output(3))
    print("")

    data_stream.print_processer_stats()

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

    data_stream.print_processer_stats()

    json = JSONPlugin()

    print("Send 5 processed data from each processer to a JSON plugin:")

    for processer in data_stream._processers:
        print("JSON Output:")
        json.process_ouput(processer.output(5))
    print("")

    data_stream.print_processer_stats()
