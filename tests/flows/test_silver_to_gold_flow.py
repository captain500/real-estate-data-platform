"""Unit tests for the silver_to_gold Prefect flow."""

from unittest.mock import call, patch

from real_estate_data_platform.models.enums import FlowStatus
from real_estate_data_platform.models.responses import SilverToGoldResult


class TestSilverToGoldFlow:
    """Tests for the silver_to_gold flow."""

    @patch("real_estate_data_platform.flows.silver_to_gold_flow.run_dbt")
    def test_success_when_both_steps_pass(self, mock_run_dbt):
        from real_estate_data_platform.flows.silver_to_gold_flow import silver_to_gold

        mock_run_dbt.return_value = ["ok"]

        result = silver_to_gold()

        assert result.status == FlowStatus.SUCCESS
        assert result.error is None
        assert mock_run_dbt.call_count == 2

        # Verify snapshot call
        snapshot_call = mock_run_dbt.call_args_list[0]
        assert snapshot_call == call(
            args=["snapshot", "--target", "dev"],
            project_dir="src/real_estate_data_platform/dbt",
            profiles_dir="src/real_estate_data_platform/dbt",
        )

        # Verify run call includes both gold models
        run_call = mock_run_dbt.call_args_list[1]
        assert run_call == call(
            args=[
                "run",
                "--select",
                "fct_rental_listings",
                "dim_neighbourhoods",
                "--target",
                "dev",
            ],
            project_dir="src/real_estate_data_platform/dbt",
            profiles_dir="src/real_estate_data_platform/dbt",
        )

    @patch("real_estate_data_platform.flows.silver_to_gold_flow.run_dbt")
    def test_error_when_snapshot_fails(self, mock_run_dbt):
        from real_estate_data_platform.flows.silver_to_gold_flow import silver_to_gold

        mock_run_dbt.side_effect = RuntimeError("snapshot boom")

        result = silver_to_gold()

        assert result.status == FlowStatus.ERROR
        assert result.error == "dbt snapshot failed"
        assert mock_run_dbt.call_count == 1

    @patch("real_estate_data_platform.flows.silver_to_gold_flow.run_dbt")
    def test_error_when_model_refresh_fails(self, mock_run_dbt):
        from real_estate_data_platform.flows.silver_to_gold_flow import silver_to_gold

        mock_run_dbt.side_effect = [["ok"], RuntimeError("model boom")]

        result = silver_to_gold()

        assert result.status == FlowStatus.ERROR
        assert result.error == "dbt run failed"
        assert mock_run_dbt.call_count == 2


class TestSilverToGoldResult:
    """Tests for the SilverToGoldResult model."""

    def test_success_result(self):
        result = SilverToGoldResult(status=FlowStatus.SUCCESS)
        assert result.status == "success"
        assert result.error is None

    def test_error_result(self):
        result = SilverToGoldResult(status=FlowStatus.ERROR, error="something broke")
        assert result.status == "error"
        assert result.error == "something broke"
