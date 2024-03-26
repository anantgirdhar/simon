import decimal
from collections import namedtuple
from configparser import ConfigParser
from pathlib import Path
from typing import List

import pytest
from simon.config import Config

ConfigField = namedtuple("ConfigField", "name type value")

SampleConfigDict = dict[str, List[ConfigField]]


@pytest.fixture
def sample_openfoam_slurm_config() -> SampleConfigDict:
    return {
        "general": [
            ConfigField("case_directory", Path, Path(".")),
            ConfigField("cluster", str, "slurm"),
        ],
        "taskqueue": [ConfigField("num_simultaneous_tasks", int, 4)],
        "openfoam": [
            ConfigField(
                "keep_every", decimal.Decimal, decimal.Decimal("0.0001")
            ),
            ConfigField(
                "compress_every", decimal.Decimal, decimal.Decimal("0.01")
            ),
            ConfigField("requeue", bool, False),
        ],
        "slurm": [
            ConfigField("job_sfile", str, "case.sbatch"),
            ConfigField(
                "compress_sfile_template", str, "compress.sbatch.template"
            ),
        ],
    }


def to_configparser(test_dict: SampleConfigDict) -> ConfigParser:
    config = ConfigParser()
    for section, field_list in test_dict.items():
        config[section] = {}
        for field in field_list:
            config[section][field.name] = str(field.value)
    return config


# Test creating a Config object


def test_config_generated_from_configparser_has_correct_fields(
    sample_openfoam_slurm_config: SampleConfigDict,
) -> None:
    cp = to_configparser(sample_openfoam_slurm_config)
    config = Config(cp)
    for section, field_list in sample_openfoam_slurm_config.items():
        for field in field_list:
            assert str(config[section][field.name]) == cp[section][field.name]


def test_config_generated_from_file_has_correct_fields(
    sample_openfoam_slurm_config: SampleConfigDict, tmp_path: Path
) -> None:
    tmp_file = tmp_path / "config.ini"
    cp = to_configparser(sample_openfoam_slurm_config)
    with open(tmp_file, "w") as f:
        cp.write(f)
    config = Config.from_file(tmp_file)
    for section, field_list in sample_openfoam_slurm_config.items():
        for field in field_list:
            assert str(config[section][field.name]) == cp[section][field.name]


# Test parameter types


def test_config_has_correct_field_types(
    sample_openfoam_slurm_config: SampleConfigDict,
) -> None:
    cp = to_configparser(sample_openfoam_slurm_config)
    config = Config(cp)
    for section, field_list in sample_openfoam_slurm_config.items():
        for field in field_list:
            assert isinstance(config[section][field.name], field.type)
