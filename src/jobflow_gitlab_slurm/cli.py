"""Offline command-line validation of site bindings and run requests."""

import argparse
import sys
from collections.abc import Sequence

import yaml
from pydantic import ValidationError

from jobflow_gitlab_slurm.loader import load_request_file, load_site_file
from jobflow_gitlab_slurm.request import validate_for_site


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="jobflow-gitlab-slurm")
    commands = parser.add_subparsers(dest="command", required=True)
    validate = commands.add_parser("validate-site", help="validate a site YAML file")
    validate.add_argument("site_file")
    validate_request = commands.add_parser(
        "validate-request", help="validate a run request against one site YAML file"
    )
    validate_request.add_argument("request_file")
    validate_request.add_argument("site_file")
    args = parser.parse_args(argv)

    label = "Site" if args.command == "validate-site" else "Run request"
    try:
        site = load_site_file(args.site_file)
        if args.command == "validate-request":
            request = validate_for_site(load_request_file(args.request_file), site)
    except (
        OSError,
        UnicodeError,
        yaml.YAMLError,
        ValidationError,
        ValueError,
    ) as error:
        print(f"{label} validation failed: {error}", file=sys.stderr)
        return 2

    if args.command == "validate-site":
        print(f"Site validation passed: {site.site_id}")
    else:
        print(
            f"Run request validation passed: {request.workflow.name} on {site.site_id}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
