from typing import Optional
import snakemake.common.tests
from snakemake_executor_plugin_tes import ExecutorSettings
from snakemake_interface_executor_plugins.settings import ExecutorSettingsBase


class TestWorkflowsBase(snakemake.common.tests.TestWorkflowsMinioPlayStorageBase):
    __test__ = True

    def get_executor(self) -> str:
        return "tes"
    
    def get_assume_shared_fs(self) -> bool:
        return False

    def get_executor_settings(self) -> Optional[ExecutorSettingsBase]:
        return ExecutorSettings(
            url="http://localhost:8000",
            user="funnel",
            password="funnel_password",
        )

    def get_remote_execution_settings(
        self,
    ) -> snakemake.settings.RemoteExecutionSettings:
        return snakemake.settings.RemoteExecutionSettings(
            seconds_between_status_checks=10,
            envvars=self.get_envvars(),
            # TODO remove once we have switched to stable snakemake for dev
            container_image="snakemake/snakemake:latest",
        )