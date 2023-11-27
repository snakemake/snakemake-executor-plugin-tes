__author__ = "Sven Twardziok, Alex Kanitz, Valentin Schneider-Lunitz, Johannes Köster"
__copyright__ = "Copyright 2023, Snakemake community"
__email__ = "johannes.koester@uni-due.de"
__license__ = "MIT"

from dataclasses import dataclass, field
import math
import os
from pathlib import Path
from typing import List, Generator, Optional

import tes

from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo
from snakemake_interface_executor_plugins.executors.remote import RemoteExecutor
from snakemake_interface_executor_plugins.settings import (
    ExecutorSettingsBase,
    CommonSettings,
)
from snakemake_interface_executor_plugins.jobs import (
    JobExecutorInterface,
)
from snakemake_interface_common.exceptions import WorkflowError


# Optional:
# define additional settings for your executor
# They will occur in the Snakemake CLI as --<executor-name>-<param-name>
# Omit this class if you don't need any.
@dataclass
class ExecutorSettings(ExecutorSettingsBase):
    url: Optional[str] = field(
        default=None,
        metadata={
            "help": "URL of TES server",
            "required": True,
        },
    )
    user: Optional[str] = field(
        default=None,
        metadata={
            "help": "TES username (either specify this or token)",
            "env_var": True,
        },
    )
    password: Optional[str] = field(
        default=None,
        metadata={
            "help": "TES password (either specify this or a token)",
            "env_var": True,
        },
    )
    token: Optional[str] = field(
        default=None,
        metadata={
            "help": "TES token (either specify this or a user/password)",
            "env_var": True,
        },
    )


# Required:
# Specify common settings shared by various executors.
common_settings = CommonSettings(
    # define whether your executor plugin executes locally
    # or remotely. In virtually all cases, it will be remote execution
    # (cluster, cloud, etc.). Only Snakemake's standard execution
    # plugins (snakemake-executor-plugin-dryrun, snakemake-executor-plugin-local)
    # are expected to specify False here.
    non_local_exec=True,
    # Define whether your executor plugin implies that there is no shared
    # filesystem (True) or not (False).
    # This is e.g. the case for cloud execution.
    implies_no_shared_fs=True,
    job_deploy_sources=True,
    pass_default_storage_provider_args=True,
    pass_default_resources_args=True,
    pass_envvar_declarations_to_cmd=True,
    auto_deploy_default_storage_provider=True,
    # wait a bit until TES backend has job info available
    init_seconds_before_status_checks=40,
)


# Required:
# Implementation of your executor
class Executor(RemoteExecutor):
    def __post_init__(self):
        self.container_workdir = Path("/tmp")
        self.tes_url = self.workflow.executor_settings.url

        self.tes_client = tes.HTTPClient(
            url=self.tes_url,
            token=self.workflow.executor_settings.token,
            user=self.workflow.executor_settings.user,
            password=self.workflow.executor_settings.password,
        )

    def run_job(self, job: JobExecutorInterface):
        # Implement here how to run a job.
        # You can access the job's resources, etc.
        # via the job object.
        # After submitting the job, you have to call
        # self.report_job_submission(job_info).
        # with job_info being of type
        # snakemake_interface_executor_plugins.executors.base.SubmittedJobInfo.

        jobscript = self.get_jobscript(job)
        self.write_jobscript(job, jobscript)
        self.logger.debug(f"[TES] Jobscript: {open(jobscript).read()}")

        # submit job here, and obtain job ids from the backend
        try:
            task = self._get_task(job, jobscript)
            tes_id = self.tes_client.create_task(task)
            self.logger.info(f"[TES] Task submitted: {tes_id}")
        except Exception as e:
            raise WorkflowError(e)

        self.report_job_submission(
            SubmittedJobInfo(
                job=job,
                external_jobid=tes_id,
                aux={"jobscript": jobscript},
            )
        )

    async def check_active_jobs(
        self, active_jobs: List[SubmittedJobInfo]
    ) -> Generator[SubmittedJobInfo, None, None]:
        # Check the status of active jobs.

        # You have to iterate over the given list active_jobs.
        # For jobs that have finished successfully, you have to call
        # self.report_job_success(job).
        # For jobs that have errored, you have to call
        # self.report_job_error(job).
        # Jobs that are still running have to be yielded.
        #
        # For queries to the remote middleware, please use
        # self.status_rate_limiter like this:
        #
        # async with self.status_rate_limiter:
        #    # query remote middleware here
        UNFINISHED_STATES = ["UNKNOWN", "INITIALIZING", "QUEUED", "RUNNING", "PAUSED"]
        ERROR_STATES = [
            "EXECUTOR_ERROR",
            "SYSTEM_ERROR",
            "CANCELED",  # TODO: really call `error_callback` on this?
        ]

        for j in active_jobs:
            async with self.status_rate_limiter:
                res = self.tes_client.get_task(j.external_jobid, view="MINIMAL")
                self.logger.debug(
                    "[TES] State of task '{id}': {state}".format(
                        id=j.external_jobid, state=res.state
                    )
                )
                if res.state in UNFINISHED_STATES:
                    yield j
                elif res.state in ERROR_STATES:
                    # TODO remove this dbg code
                    import sys

                    print(
                        open(os.environ["GITHUB_WORKSPACE"] + "/funnel.log").read(),
                        file=sys.stderr,
                    )
                    self.report_job_error(j)
                elif res.state == "COMPLETE":
                    self.report_job_success(j)

    def cancel_jobs(self, active_jobs: List[SubmittedJobInfo]):
        # Cancel all active jobs.
        # This method is called when Snakemake is interrupted.
        for job_info in active_jobs:
            try:
                self.tes_client.cancel_task(job_info.external_jobid)
                self.logger.info(f"[TES] Task canceled: {job_info.external_jobid}")
            except Exception:
                self.logger.info(
                    "[TES] Canceling task failed. This may be because the job is "
                    "already in a terminal state."
                )

    def get_job_exec_prefix(self, job: JobExecutorInterface):
        return "mkdir /tmp/conda && cd /tmp"

    def _check_file_in_dir(self, checkdir, f):
        if checkdir:
            checkdir = checkdir.rstrip("/")
            if not f.startswith(checkdir):
                direrrmsg = (
                    "All files including Snakefile, "
                    + "conda env files, rule script files, output files "
                    + "must be in the same working directory: {} vs {}"
                )
                raise WorkflowError(direrrmsg.format(checkdir, f))

    def _get_members_path(self, overwrite_path, f) -> str:
        if overwrite_path:
            members_path = overwrite_path
        else:
            members_path = self.container_workdir / os.path.relpath(f)
        return str(members_path)

    def _prepare_file(
        self,
        iofile,
        overwrite_path=None,
        checkdir=None,
        pass_content=False,
        type="Input",
    ):
        # TODO: handle FTP files
        max_file_size = 131072
        if type not in ["Input", "Output"]:
            raise ValueError("Value for 'model' has to be either 'Input' or 'Output'.")

        members = {}

        # Handle remote files
        if hasattr(iofile, "is_storage") and iofile.is_storage:
            return None

        # Handle local files
        else:
            f = os.path.abspath(iofile)

            self._check_file_in_dir(checkdir, f)

            members["path"] = self._get_members_path(overwrite_path, f)

            members["url"] = "file://" + f
            if pass_content:
                source_file_size = os.path.getsize(f)
                if source_file_size > max_file_size:
                    self.logger.warning(
                        "Will not pass file '{f}' by content, as it exceeds the "
                        "minimum supported file size of {max_file_size} bytes "
                        "defined in the TES specification. Will try to upload "
                        "file instead.".format(f=f, max_file_size=max_file_size)
                    )
                else:
                    with open(f) as stream:
                        members["content"] = stream.read()
                    members["url"] = None

        model = getattr(tes.models, type)
        self.logger.warning(members)
        return model(**members)

    def _get_task_description(self, job: JobExecutorInterface):
        description = ""
        if job.is_group():
            msgs = [i.message for i in job.jobs if i.message]
            if msgs:
                description = " & ".join(msgs)
        else:
            if job.message:
                description = job.message

        return description

    def _get_task_inputs(self, job: JobExecutorInterface, jobscript, checkdir):
        inputs = []

        # add input files to inputs
        for i in job.input:
            obj = self._prepare_file(iofile=i, checkdir=checkdir)
            if obj:
                inputs.append(obj)

        # add jobscript to inputs
        inputs.append(
            self._prepare_file(
                iofile=jobscript,
                overwrite_path=os.path.join(self.container_workdir, "run_snakemake.sh"),
                checkdir=checkdir,
                pass_content=True,
            )
        )

        return inputs

    def _append_task_outputs(self, outputs, files, checkdir):
        for file in files:
            obj = self._prepare_file(iofile=file, checkdir=checkdir, type="Output")
            if obj:
                outputs.append(obj)
        return outputs

    def _get_task_outputs(self, job: JobExecutorInterface, checkdir):
        outputs = []
        # add output files to outputs
        outputs = self._append_task_outputs(outputs, job.output, checkdir)

        # add log files to outputs
        if job.log:
            outputs = self._append_task_outputs(outputs, job.log, checkdir)

        # add benchmark files to outputs
        if hasattr(job, "benchmark") and job.benchmark:
            outputs = self._append_task_outputs(outputs, [job.benchmark], checkdir)

        return outputs

    def _get_task_executors(self):
        executors = []
        self.logger.debug(
            "[TES] Container image: "
            f"{self.workflow.remote_execution_settings.container_image}"
        )
        executors.append(
            tes.models.Executor(
                image=self.workflow.remote_execution_settings.container_image,
                command=[  # TODO: info about what is executed is opaque
                    "/bin/bash",
                    os.path.join(self.container_workdir, "run_snakemake.sh"),
                ],
                workdir=str(self.container_workdir),
            )
        )
        return executors

    def _get_task(self, job: JobExecutorInterface, jobscript):
        checkdir, _ = os.path.split(self.snakefile)

        task = {}
        task["name"] = job.format_wildcards(self.jobname)
        task["description"] = self._get_task_description(job)
        task["inputs"] = self._get_task_inputs(job, jobscript, checkdir)
        task["outputs"] = self._get_task_outputs(job, checkdir)
        task["executors"] = self._get_task_executors()
        task["resources"] = tes.models.Resources()

        # define resources
        if job.resources.get("_cores") is not None:
            task["resources"].cpu_cores = job.resources["_cores"]
        if job.resources.get("mem_mb") is not None:
            task["resources"].ram_gb = math.ceil(job.resources["mem_mb"] / 1000)
        if job.resources.get("disk_mb") is not None:
            task["resources"].disk_gb = math.ceil(job.resources["disk_mb"] / 1000)

        tes_task = tes.Task(**task)
        self.logger.debug(f"[TES] Built task: {tes_task}")
        return tes_task
