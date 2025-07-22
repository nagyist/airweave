"""Base config class."""

from typing import Optional, get_args, get_origin

from pydantic import BaseModel, model_validator
from pydantic_core import PydanticUndefined


class BaseConfig(BaseModel):
    """Base config class."""

    pass


class ConfigField(BaseModel):
    """Config field model."""

    name: str
    title: str
    description: Optional[str] = None
    type: str
    required: bool = True  # Default to True for backward compatibility


_type_map = {str: "string", int: "number", float: "number", bool: "boolean"}


class Fields(BaseModel):
    """Fields model."""

    fields: list[ConfigField]

    @classmethod
    def from_config_class(cls, config_class: type[BaseConfig]) -> "Fields":
        """Create fields from config class."""
        fields = []
        for field_name, field_info in config_class.model_fields.items():
            # Get the actual type, handling Optional types
            annotation = field_info.annotation
            is_optional = False
            if get_origin(annotation) is Optional:
                annotation = get_args(annotation)[0]
                is_optional = True

            # Get the base type if it's a Field
            if hasattr(annotation, "__origin__"):
                annotation = annotation.__origin__

            # Map the type to string representation
            type_str = _type_map.get(annotation, "string")  # Default to string if type not found

            # Determine if field is required
            # A field is required if it has no default and is not Optional
            has_default = field_info.default is not PydanticUndefined
            is_required = not has_default and not is_optional

            fields.append(
                ConfigField(
                    name=field_name,
                    title=field_info.title or field_name,
                    description=field_info.description,
                    type=type_str,
                    required=is_required,
                )
            )
        return Fields(fields=fields)


class ConfigValues(BaseModel):
    """Config values model.

    Implements "flat dictionary" semantics, where no values are dictionaries.
    """

    # Allow arbitrary fields
    model_config = {
        "extra": "allow",
    }

    @model_validator(mode="after")
    def validate_config_values(self):
        """Validate that no values are dictionaries (depth 0)."""
        for key, value in self.__dict__.items():
            if isinstance(value, dict):
                raise ValueError(f"Value for '{key}' must not be a dictionary (depth 0 only)")
        return self
