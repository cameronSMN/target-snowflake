"""JSON Lines Record Batcher."""

from __future__ import annotations

import gzip
import json
import decimal
import typing as t
from uuid import uuid4
from target_snowflake import flattening

from singer_sdk.batch import BaseBatcher, lazy_chunked_generator

from singer_sdk.helpers._batch import (
    BatchConfig
)

__all__ = ["CSVBatcher"]


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super().default(o)

class CSVBatcher(BaseBatcher):
    """CSV Record Batcher."""
    def __init__(self, 
                tap_name: str,
                stream_name: str,
                batch_config: BatchConfig,
                schema: t.Dict, 
                data_flattening_max_level: int = 0):
        self.schema = schema
        self.data_flattening_max_level = data_flattening_max_level
        """Initialize BaseBatcher."""
        super().__init__(
            tap_name=tap_name,
            stream_name=stream_name,
            batch_config=batch_config,
        )

    def _record_to_csv_line(self, record: dict,
                        schema: dict,
                        data_flattening_max_level: int = 0) -> str:
        """
        Transforms a record message to a CSV line

        Args:
            record: Dictionary that represents a csv line. Dict key is column name, value is the column value
            schema: JSONSchema of the record
            data_flattening_max_level: Max level of auto flattening if a record message has nested objects. (Default: 0)

        Returns:
            string of csv line
        """
        flatten_record = flattening.flatten_record(record, schema, max_level=data_flattening_max_level)
        return ','.join(
            [
                json.dumps(flatten_record[column], ensure_ascii=False, cls=DecimalEncoder) if column in flatten_record and (
                        flatten_record[column] == 0 or flatten_record[column]) else ''
                for column, meta in schema["properties"].items()
            ]
        )


    def get_batches(
        self,
        records: t.Iterator[dict],
    ) -> t.Iterator[list[str]]:
        """Yield manifest of batches.

        Args:
            records: The records to batch.

        Yields:
            A list of file paths (called a manifest).
        """
        sync_id = f"{self.tap_name}--{self.stream_name}-{uuid4()}"
        prefix = self.batch_config.storage.prefix or ""
        for i, chunk in enumerate(
            lazy_chunked_generator(
                records,
                self.batch_config.batch_size,
            ),
            start=1,
        ):
            filename = f"{prefix}{sync_id}-{i}.csv.gz"
            with self.batch_config.storage.fs(create=True) as fs:
                # TODO: Determine compression from config.
                # with fs.open(filename, "wb") as outfile:
                #     for record in chunk:
                #         csv_line = self._record_to_csv_line(record, self.schema, self.data_flattening_max_level)
                #         outfile.write(bytes(csv_line + '\n', 'UTF-8'))

                with fs.open(filename, "wb") as f, gzip.GzipFile(
                    fileobj=f,
                    mode="wb",
                ) as gz:
                    for record in chunk:
                        csv_line = self._record_to_csv_line(record, self.schema, self.data_flattening_max_level)
                        gz.write(bytes(csv_line + '\n', 'UTF-8'))
                    
                file_url = fs.geturl(filename)
            yield [file_url]



