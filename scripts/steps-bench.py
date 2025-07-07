from subprocess import Popen, run
import os
import logging
import logging.config
import time

logging.config.dictConfig(
    {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "simple": {
                "format": "[%(asctime)s|%(levelname)s|%(name)s|%(filename)s:%(lineno)d] %(message)s",
                "datefmt": "%Y-%m-%dT%H:%M:%S%z",
            }
        },
        "handlers": {
            "stdout": {
                "class": "logging.StreamHandler",
                "formatter": "simple",
                "stream": "ext://sys.stdout",
                "level": "DEBUG",
            }
        },
        "root": {"level": "DEBUG", "handlers": ["stdout"]},
    }
)
logger = logging.getLogger("bench")


def main():
    logger.info("Building working_version")
    cmake_build = Popen(
        [
            "cmake",
            "--build",
            "cmake-build-debug",
            "--target",
            "working_version",
            "--",
            "-j",
            f"{os.cpu_count() or 1}",
        ]
    )
    cmake_build.wait()
    if cmake_build.returncode:
        exit(cmake_build.returncode)

    logger.info("Generating workload")
    load_gen = run(
        ["./bin/load_gen", "-I", str(1_000_000)],
        capture_output=True,
    )
    load_gen.check_returncode()

    logger.info("Benchmarking")
    os.makedirs("logs/", exist_ok=True)

    filename = f"logs/steps-{int(time.time())}.log"
    with open(filename, "w") as f:
        working_version = run(
            [
                "./bin/working_version",
            ],
            stdout=f,
        )
    working_version.check_returncode()

    logger.info(f"done benchmarking, logs written to {filename}")


if __name__ == "__main__":
    main()
