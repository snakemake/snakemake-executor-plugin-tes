from typing import Optional
import snakemake.common.tests
from snakemake_executor_plugin_tes import ExecutorSettings
from snakemake_interface_executor_plugins.settings import ExecutorSettingsBase


class TestWorkflowsBase(snakemake.common.tests.TestWorkflowsLocalStorageBase):
    __test__ = True

    def get_executor(self) -> str:
        return "tes"

    def get_executor_settings(self) -> Optional[ExecutorSettingsBase]:
        return ExecutorSettings(
            url="http://localhost:8000",
            user="funnel",
            password="funnel_password",
        )
