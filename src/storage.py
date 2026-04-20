from __future__ import annotations

import gzip
import io
import threading
from pathlib import Path
from typing import BinaryIO

from warcio.warcwriter import WARCWriter


class WARCStorage:
    def __init__(
        self,
        output_dir: Path,
        prefix: str = "corpus",
        pages_per_file: int = 1000,
        flush_every: int = 100,
    ) -> None:
        self.output_dir = output_dir
        self.prefix = prefix
        self.pages_per_file = pages_per_file
        self.flush_every = max(1, flush_every)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self._lock = threading.Lock()
        self._file_index = 0
        self._pages_in_current_file = 0
        self._gzip_file: BinaryIO | None = None
        self._writer: WARCWriter | None = None
        self._open_new_file()

    def _open_new_file(self) -> None:
        self._file_index += 1
        filename = f"{self.prefix}_{self._file_index:05d}.warc.gz"
        filepath = self.output_dir / filename
        self._gzip_file = gzip.open(filepath, "wb")
        self._writer = WARCWriter(self._gzip_file, gzip=False)
        self._pages_in_current_file = 0

    def write_response(self, url: str, html_bytes: bytes, content_type: str = "text/html; charset=utf-8") -> None:
        with self._lock:
            assert self._writer is not None
            assert self._gzip_file is not None

            if self._pages_in_current_file >= self.pages_per_file:
                self._gzip_file.flush()
                self._gzip_file.close()
                self._open_new_file()

            payload = io.BytesIO(html_bytes)
            record = self._writer.create_warc_record(
                uri=url,
                record_type="resource",
                payload=payload,
                warc_headers_dict={
                    "WARC-Identified-Payload-Type": content_type,
                },
            )
            self._writer.write_record(record)
            self._pages_in_current_file += 1

            if self._pages_in_current_file % self.flush_every == 0:
                self._gzip_file.flush()

    def close(self) -> None:
        with self._lock:
            if self._gzip_file is not None:
                self._gzip_file.flush()
                self._gzip_file.close()
                self._gzip_file = None
                self._writer = None