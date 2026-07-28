#!/usr/bin/env python3
"""Export the isolated Docker Compose project for an integration run."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from pathlib import Path

PROJECT_PREFIX = "sdk-python"
RUN_COMPONENT = re.compile(r"[0-9]+")


def compose_project_name(environment: Mapping[str, str]) -> str:
    """Return a deterministic project name for one workflow run attempt."""
    components: list[str] = []
    for variable in ("GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT"):
        value = environment.get(variable, "")
        if RUN_COMPONENT.fullmatch(value) is None:
            raise RuntimeError(f"{variable} must be a non-empty decimal workflow identity")
        components.append(value)

    return "-".join((PROJECT_PREFIX, *components))


def export_project(project: str, github_environment: Path) -> None:
    with github_environment.open("a", encoding="utf-8") as environment_file:
        environment_file.write(f"COMPOSE_PROJECT_NAME={project}\n")


def main() -> int:
    github_environment = os.environ.get("GITHUB_ENV")
    if not github_environment:
        raise RuntimeError("GITHUB_ENV must name the CI environment export file")

    project = compose_project_name(os.environ)
    export_project(project, Path(github_environment))
    print(f"Docker Compose project: {project}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
