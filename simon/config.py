import decimal
import re
from collections import namedtuple
from configparser import ConfigParser, SectionProxy
from pathlib import Path
from typing import Any, Iterator, List, Tuple

# NOTE: All paths should be relative to the case directory

VALID_IDENTIFIER = re.compile(r"[a-zA-Z_]+[a-zA-Z0-9_]*")

# Define the structure of the config file
# Each config parameter should define a field name, a way to coerce a string
# into the appropriate type, and a value that will be printed when the config
# file is initialized
ConfigField = namedtuple("ConfigField", "name coercer sample_value")
_FIELDS = {
    "general": [
        ConfigField("case_directory", Path, Path(".")),
        ConfigField("cluster", str, "slurm|local"),
    ],
    "taskqueue": [
        ConfigField("num_simultaneous_tasks", int, 4),
    ],
    "openfoam": [
        ConfigField("keep_every", decimal.Decimal, decimal.Decimal("0.0001")),
        ConfigField(
            "compress_every", decimal.Decimal, decimal.Decimal("0.01")
        ),
        ConfigField("requeue", lambda x: x == "True", True),
    ],
    "slurm": [
        ConfigField("job_sfile", str, "case.sbatch"),
        ConfigField(
            "compress_sfile_template", str, "compress.sbatch.template"
        ),
    ],
}


class TypedSectionProxy:
    def __init__(
        self, name: str, fields: List[ConfigField], values: SectionProxy
    ) -> None:
        self.name = name
        self._fields: dict[str, Any] = {}
        for field in fields:
            self._fields[field.name] = field.coercer(values[field.name])

    def __getattr__(self, key: str) -> Any:
        if key in self.__dict__:
            return self.__dict__[key]
        if key not in self._fields:
            raise AttributeError(f"{key} not found in {self.name}")
        return self._fields[key]

    def __getitem__(self, key: str) -> Any:
        return self._fields[key]

    def __setattr__(self, key: str, value: Any) -> None:
        if key == "name":
            self.__dict__["name"] = value
        elif key == "_fields":
            self.__dict__["_fields"] = value
        else:
            self._fields[key] = value

    def __setitem__(self, key: str, value: Any) -> None:
        self._fields[key] = value

    def __iter__(self) -> Iterator[Tuple[str, Any]]:
        return iter(self._fields.items())

    def __str__(self) -> str:
        section_string = ""
        section_string += f"[{self.name}]\n"
        for field, value in self._fields.items():
            section_string += f"{field}: {value}\n"
        return section_string.strip()


class Config:
    def __init__(self, config: ConfigParser) -> None:
        self._sections: dict[str, TypedSectionProxy] = {}
        for section_name, field_list in _FIELDS.items():
            self._sections[section_name] = TypedSectionProxy(
                section_name, field_list, config[section_name]
            )

    def __getattr__(self, key: str) -> Any:
        if key in self.__dict__:
            return self.__dict__[key]
        if key not in self._sections:
            raise AttributeError(f"{key} not found in config")
        return self._sections[key]

    def __getitem__(self, section: str) -> TypedSectionProxy:
        return self._sections[section]

    def __setattr__(self, key: str, value: Any) -> None:
        if key == "_sections":
            self.__dict__["_sections"] = value
        else:
            raise NotImplementedError("Cannot set properties on this config")

    def __setitem__(self, key: str, value: Any) -> None:
        raise NotImplementedError("Cannot set properties on this config")

    @staticmethod
    def from_file(filepath: Path) -> "Config":
        config = ConfigParser()
        config.read(filepath)
        return Config(config)

    def to_file(self, filepath: Path) -> None:
        if filepath.is_dir():
            raise ValueError(f"{filepath} is a directory")
        if filepath.is_file():
            raise ValueError(f"{filepath} already exists")
        config = ConfigParser()
        for section_name, section_values in self._sections.items():
            config[section_name] = {}
            for field, value in section_values:
                config[section_name][field] = str(value)
        with open(filepath, "w") as configfile:
            self.config.write(configfile)

    @staticmethod
    def write_sample(filepath: Path) -> None:
        if filepath.is_dir():
            raise ValueError(f"{filepath} is a directory")
        if filepath.is_file():
            raise ValueError(f"{filepath} already exists")
        config = ConfigParser()
        for section, field_list in _FIELDS.items():
            config[section] = {}
            for field in field_list:
                config[section][field.name] = str(field.sample_value)
        with open(filepath, "w") as configfile:
            config.write(configfile)

    def __str__(self) -> str:
        config_string = ""
        for section in self._sections.values():
            config_string += str(section) + "\n\n"
        return config_string.strip()
