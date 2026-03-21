"""Unit tests for the run_dbt Prefect task."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_PATCH_PREFIX = "real_estate_data_platform.tasks.run_dbt"


class TestRunDbt:
    """Tests for the run_dbt task."""

    @patch(f"{_PATCH_PREFIX}.get_run_logger")
    @patch(f"{_PATCH_PREFIX}.PrefectDbtRunner")
    def test_invokes_runner_with_correct_args(self, mock_runner_cls, _mock_logger):
        from real_estate_data_platform.tasks.run_dbt import run_dbt

        mock_runner = MagicMock()
        mock_runner_cls.return_value = mock_runner

        run_dbt.fn(
            args=["snapshot"],
            project_dir="dbt",
            profiles_dir="dbt",
        )

        mock_runner.invoke.assert_called_once_with(["snapshot"])

    @patch(f"{_PATCH_PREFIX}.get_run_logger")
    @patch(f"{_PATCH_PREFIX}.PrefectDbtRunner")
    def test_propagates_dbt_error(self, mock_runner_cls, _mock_logger):
        from real_estate_data_platform.tasks.run_dbt import run_dbt

        mock_runner = MagicMock()
        mock_runner.invoke.side_effect = RuntimeError("dbt failed")
        mock_runner_cls.return_value = mock_runner

        with pytest.raises(RuntimeError, match="dbt failed"):
            run_dbt.fn(
                args=["snapshot"],
                project_dir="dbt",
                profiles_dir="dbt",
            )

    @patch(f"{_PATCH_PREFIX}.get_run_logger")
    @patch(f"{_PATCH_PREFIX}.PrefectDbtRunner")
    def test_passes_settings_to_runner(self, mock_runner_cls, _mock_logger):
        from real_estate_data_platform.tasks.run_dbt import run_dbt

        mock_runner = MagicMock()
        mock_runner_cls.return_value = mock_runner

        run_dbt.fn(
            args=["run", "--select", "rentals_listings"],
            project_dir="/custom/path",
            profiles_dir="/custom/profiles",
        )

        settings_arg = mock_runner_cls.call_args.kwargs["settings"]
        assert settings_arg.project_dir == Path("/custom/path")
        assert settings_arg.profiles_dir == Path("/custom/profiles")
        mock_runner.invoke.assert_called_once_with(["run", "--select", "rentals_listings"])
