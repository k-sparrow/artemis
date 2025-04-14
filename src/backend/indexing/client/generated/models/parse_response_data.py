from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.parse_response_data_json_output_type_0 import (
        ParseResponseDataJsonOutputType0,
    )


T = TypeVar("T", bound="ParseResponseData")


@_attrs_define
class ParseResponseData:
    """
    Attributes:
        output (str):
        json_output (Union['ParseResponseDataJsonOutputType0', None, Unset]):
    """

    output: str
    json_output: Union["ParseResponseDataJsonOutputType0", None, Unset] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.parse_response_data_json_output_type_0 import (
            ParseResponseDataJsonOutputType0,
        )

        output = self.output

        json_output: Union[None, Unset, dict[str, Any]]
        if isinstance(self.json_output, Unset):
            json_output = UNSET
        elif isinstance(self.json_output, ParseResponseDataJsonOutputType0):
            json_output = self.json_output.to_dict()
        else:
            json_output = self.json_output

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "output": output,
            }
        )
        if json_output is not UNSET:
            field_dict["json_output"] = json_output

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.parse_response_data_json_output_type_0 import (
            ParseResponseDataJsonOutputType0,
        )

        d = dict(src_dict)
        output = d.pop("output")

        def _parse_json_output(
            data: object,
        ) -> Union["ParseResponseDataJsonOutputType0", None, Unset]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                json_output_type_0 = ParseResponseDataJsonOutputType0.from_dict(data)

                return json_output_type_0
            except:  # noqa: E722
                pass
            return cast(Union["ParseResponseDataJsonOutputType0", None, Unset], data)

        json_output = _parse_json_output(d.pop("json_output", UNSET))

        parse_response_data = cls(
            output=output,
            json_output=json_output,
        )

        parse_response_data.additional_properties = d
        return parse_response_data

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
