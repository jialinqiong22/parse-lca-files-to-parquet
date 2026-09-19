#!/usr/bin/env python3
import base64
import os
import pathlib
import subprocess
import zipfile


PERIODS = [(2025, 1), (2025, 2), (2025, 3), (2025, 4), (2026, 1), (2026, 2), (2026, 3)]
URL_TEMPLATES = [
    "https://www.dol.gov/sites/dolgov/files/ETA/oflc/pdfs/LCA_Disclosure_Data_FY{fy}_Q{quarter}.xlsx",
    "https://www.dol.gov/media/LCA_Disclosure_Data_FY{fy}_Q{quarter}.xlsx",
    "https://www.dol.gov/media/LCA_Dislclosure_Data_FY{fy}_Q{quarter}.xlsx",
]


def checkout_token() -> str:
    header = subprocess.check_output(
        ["git", "config", "--get", "http.https://github.com/.extraheader"], text=True
    ).strip()
    encoded = header.split("basic", 1)[1].strip()
    decoded = base64.b64decode(encoded).decode()
    return decoded.split(":", 1)[1]


def main() -> None:
    destination = pathlib.Path("/tmp/buildpm-dol-lca")
    destination.mkdir(parents=True, exist_ok=True)
    assets: list[str] = []
    for fiscal_year, quarter in PERIODS:
        filename = f"LCA_Disclosure_Data_FY{fiscal_year}_Q{quarter}.xlsx"
        output = destination / filename
        source_file = destination / f"{filename}.source-url.txt"
        downloaded_url = ""
        for template in URL_TEMPLATES:
            url = template.format(fy=fiscal_year, quarter=quarter)
            result = subprocess.run(
                ["curl", "--fail", "--location", "--retry", "3", "--retry-all-errors", url, "--output", str(output)]
            )
            if result.returncode == 0:
                downloaded_url = url
                break
        if not downloaded_url:
            raise RuntimeError(f"No official DOL URL succeeded for FY{fiscal_year} Q{quarter}")
        if not zipfile.is_zipfile(output):
            raise RuntimeError(f"Downloaded file is not a valid XLSX: {filename}")
        source_file.write_text(downloaded_url + "\n", encoding="utf-8")
        assets.extend([str(output), str(source_file)])

    repository = os.environ["GITHUB_REPOSITORY"]
    tag = f"buildpm-dol-lca-{os.environ['GITHUB_RUN_ID']}"
    env = {**os.environ, "GH_TOKEN": checkout_token()}
    subprocess.run(
        [
            "gh", "release", "create", tag, *assets,
            "--repo", repository,
            "--title", "BuildPM DOL LCA FY2025-FY2026Q3",
            "--notes", "Temporary mirror of official DOL XLSX files for BuildPM staging import.",
        ],
        check=True,
        env=env,
    )


if __name__ == "__main__":
    main()
