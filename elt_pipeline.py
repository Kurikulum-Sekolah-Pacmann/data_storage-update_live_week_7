import subprocess as sp
import os
import luigi
import datetime
from typing import TypeVar, Callable

T = TypeVar("T")
DBT_PROJECT_PATH = "/home/laode/pacmann/course/data-storage/week-6/pacflight" #Sesuaikan

class GlobalParams(luigi.Config):
    CurrentTimeStampParams = luigi.DateSecondParameter(default=datetime.datetime.now())


class dbtDebug(luigi.Task):
    get_current_timestamp = GlobalParams().CurrentTimeStampParams

    def requires(self) -> None:  # type: ignore
        pass

    def output(self) -> T:  # type: ignore
        return luigi.LocalTarget(
            f"/home/laode/pacmann/course/data-storage/week-6/logs/dbt_debug/dbt_debug_logs_{self.get_current_timestamp}.log"
        )  # type: ignore

    def run(self) -> None:
        try:
            with open(self.output().path, "a") as f:
                p1 = sp.run(
                    f"cd {DBT_PROJECT_PATH} && dbt debug",
                    stdout=f,
                    stderr=sp.PIPE,
                    text=True,
                    shell=True,
                    check=True,
                )

                if p1.returncode == 0:
                    print("Success Run dbt debug process")

                else:
                    print("Failed to run dbt debug process")

        except Exception as e:
            raise e


class dbtDeps(luigi.Task):
    get_current_timestamp = GlobalParams().CurrentTimeStampParams

    def requires(self) -> Callable:  # type: ignore
        return dbtDebug()  # type: ignore

    def output(self) -> T:  # type: ignore
        return luigi.LocalTarget(
            f"/home/laode/pacmann/course/data-storage/week-6/logs/dbt_deps/dbt_deps_logs_{self.get_current_timestamp}.log"
        )  # type: ignore

    def run(self) -> None:
        try:
            with open(self.output().path, "a") as f:
                p1 = sp.run(
                    f"cd {DBT_PROJECT_PATH} && dbt deps",
                    stdout=f,
                    stderr=sp.PIPE,
                    text=True,
                    shell=True,
                    check=True,
                )

                if p1.returncode == 0:
                    print("Success Run dbt deps process")

                else:
                    print("Failed to run dbt deps process")

        except Exception as e:
            raise e


class dbtRun(luigi.Task):
    get_current_timestamp = GlobalParams().CurrentTimeStampParams

    def requires(self) -> Callable:  # type: ignore
        return dbtDeps()  # type: ignore

    def output(self) -> T:  # type: ignore
        return luigi.LocalTarget(
            f"/home/laode/pacmann/course/data-storage/week-6/logs/dbt_run/dbt_run_logs_{self.get_current_timestamp}.log"
        )  # type: ignore

    def run(self) -> None:
        try:
            with open(self.output().path, "a") as f:
                p1 = sp.run(
                    f"cd {DBT_PROJECT_PATH} && dbt run",
                    stdout=f,
                    stderr=sp.PIPE,
                    text=True,
                    shell=True,
                    check=True,
                )

                if p1.returncode == 0:
                    print("Success running dbt data model")

                else:
                    print("Failed to run dbt data model")

        except Exception as e:
            raise e


class dbtTest(luigi.Task):
    get_current_timestamp = GlobalParams().CurrentTimeStampParams

    def requires(self) -> Callable:  # type: ignore
        return dbtRun()  # type: ignore

    def output(self) -> T:  # type: ignore
        return luigi.LocalTarget(
            f"/home/laode/pacmann/course/data-storage/week-6/logs/dbt_test/dbt_test_logs_{self.get_current_timestamp}.log"
        )  # type: ignore

    def run(self) -> None:
        try:
            with open(self.output().path, "a") as f:
                p1 = sp.run(
                    f"cd {DBT_PROJECT_PATH} && dbt test",
                    stdout=f,
                    stderr=sp.PIPE,
                    text=True,
                    shell=True,
                    check=True,
                )

                if p1.returncode == 0:
                    print("Success running testing and create a constraints")

                else:
                    print("Failed running testing and create constraints")

        except Exception as e:
            raise e


if __name__ == "__main__":
    luigi.build([dbtDebug(), dbtDeps(), dbtRun(), dbtTest()], local_scheduler=True)