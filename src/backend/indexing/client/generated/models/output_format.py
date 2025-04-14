from enum import Enum


class OutputFormat(str, Enum):
    HTML = "html"
    MARKDOWN = "markdown"
    TEXT = "text"

    def __str__(self) -> str:
        return str(self.value)
