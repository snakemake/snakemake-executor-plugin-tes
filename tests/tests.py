from typing import Optional
import snakemake.common.tests
from snakemake_executor_plugin_tes import ExecutorSettings
from snakemake_interface_executor_plugins import ExecutorSettingsBase


class TestWorkflowsBase(snakemake.common.tests.TestWorkflowsBase):
    __test__ = True

    def get_executor(self) -> str:
        return "tes"

    def get_executor_settings(self) -> Optional[ExecutorSettingsBase]:
        return ExecutorSettings(
            url="http://localhost:8000",
            user="funnel",
            password="funnel_password",
        )

    def get_default_remote_provider(self) -> Optional[str]:
        # Return name of default remote provider if required for testing,
        # otherwise None.
        return None

    def get_default_remote_prefix(self) -> Optional[str]:
        # Return default remote prefix if required for testing,
        # otherwise None.
        return None
