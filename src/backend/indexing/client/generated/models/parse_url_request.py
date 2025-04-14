from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define

from ..models.output_format import OutputFormat
from ..types import UNSET, Unset

T = TypeVar("T", bound="ParseUrlRequest")


@_attrs_define
class ParseUrlRequest:
    """
    Attributes:
        url (str): Download url for input file
        include_json (Union[Unset, bool]): Include a json representation of the document in the response Default: False.
        output_format (Union[Unset, OutputFormat]):
    """

    url: str
    include_json: Union[Unset, bool] = False
    output_format: Union[Unset, OutputFormat] = UNSET

    def to_dict(self) -> dict[str, Any]:
        url = self.url

        include_json = self.include_json

        output_format: Union[Unset, str] = UNSET
        if not isinstance(self.output_format, Unset):
            output_format = self.output_format.value

        field_dict: dict[str, Any] = {}
        field_dict.update(
            {
                "url": url,
            }
        )
        if include_json is not UNSET:
            field_dict["include_json"] = include_json
        if output_format is not UNSET:
            field_dict["output_format"] = output_format

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        url = d.pop("url")

        include_json = d.pop("include_json", UNSET)

        _output_format = d.pop("output_format", UNSET)
        output_format: Union[Unset, OutputFormat]
        if isinstance(_output_format, Unset):
            output_format = UNSET
        else:
            output_format = OutputFormat(_output_format)

        parse_url_request = cls(
            url=url,
            include_json=include_json,
            output_format=output_format,
        )

        return parse_url_request
