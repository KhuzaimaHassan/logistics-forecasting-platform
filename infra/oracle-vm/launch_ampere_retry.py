#!/usr/bin/env python3
"""
Automated OCI Always Free Ampere A1 Instance Provisioning Script with Retry Loop.

Repeatedly attempts to launch a VM.Standard.A1.Flex instance on Oracle Cloud
Infrastructure until capacity becomes available, handling "Out of host capacity" (499/500)
gracefully without crashing.
"""

import argparse
import datetime
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path


def log(msg: str):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)


def check_oci_cli():
    if not shutil.which("oci"):
        log("ERROR: OCI CLI ('oci') not found in PATH.")
        log("Please install the OCI CLI first. See setup instructions.")
        sys.exit(1)


def run_oci_json(args: list[str]) -> tuple[int, dict | str]:
    cmd = ["oci"] + args + ["--output", "json"]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        return proc.returncode, proc.stderr.strip()
    try:
        data = json.loads(proc.stdout)
        return 0, data
    except json.JSONDecodeError:
        return 0, proc.stdout.strip()


def get_availability_domains(compartment_id: str) -> list[str]:
    log("Discovering Availability Domains...")
    code, out = run_oci_json(
        ["iam", "availability-domain", "list", "--compartment-id", compartment_id]
    )
    if code != 0:
        log(f"Warning: Could not list availability domains: {out}")
        return []
    ads = [item["name"] for item in out.get("data", [])]
    log(f"Found {len(ads)} Availability Domain(s): {', '.join(ads)}")
    return ads


def get_latest_ubuntu_arm_image(compartment_id: str) -> str | None:
    log("Resolving latest Canonical Ubuntu LTS ARM64 image OCID...")
    code, out = run_oci_json(
        [
            "compute",
            "image",
            "list",
            "--compartment-id",
            compartment_id,
            "--operating-system",
            "Canonical Ubuntu",
            "--shape",
            "VM.Standard.A1.Flex",
            "--sort-by",
            "TIMECREATED",
            "--sort-order",
            "DESC",
        ]
    )
    if code != 0 or not isinstance(out, dict):
        log(f"Warning: Could not automatically query images: {out}")
        return None

    images = out.get("data", [])
    # Look for Ubuntu 24.04 or 22.04 LTS aarch64
    for img in images:
        display_name = img.get("display-name", "")
        if "aarch64" in display_name.lower() or "arm64" in display_name.lower():
            log(f"Selected image: {display_name} ({img['id']})")
            return img["id"]

    if images:
        fallback = images[0]
        log(
            f"Using latest Canonical image: {fallback.get('display-name')} ({fallback['id']})"
        )
        return fallback["id"]

    return None


def get_subnets(compartment_id: str) -> list[dict]:
    code, out = run_oci_json(
        ["network", "subnet", "list", "--compartment-id", compartment_id]
    )
    if code != 0 or not isinstance(out, dict):
        return []
    return out.get("data", [])


def attempt_launch(
    compartment_id: str,
    availability_domain: str,
    image_id: str,
    subnet_id: str,
    ssh_key_path: str,
    ocpus: int = 2,
    memory_in_gbs: int = 12,
    boot_volume_size_in_gbs: int = 100,
    display_name: str = "logistics-ampere-a1",
) -> tuple[bool, dict | str]:
    shape_config = json.dumps({"ocpus": ocpus, "memoryInGBs": memory_in_gbs})

    cmd = [
        "oci",
        "compute",
        "instance",
        "launch",
        "--compartment-id",
        compartment_id,
        "--availability-domain",
        availability_domain,
        "--shape",
        "VM.Standard.A1.Flex",
        "--shape-config",
        shape_config,
        "--image-id",
        image_id,
        "--subnet-id",
        subnet_id,
        "--assign-public-ip",
        "true",
        "--boot-volume-size-in-gbs",
        str(boot_volume_size_in_gbs),
        "--ssh-authorized-keys-file",
        ssh_key_path,
        "--display-name",
        display_name,
        "--output",
        "json",
    ]

    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode == 0:
        try:
            return True, json.loads(proc.stdout)
        except json.JSONDecodeError:
            return True, proc.stdout
    else:
        return False, proc.stderr.strip()


def is_capacity_error(err_msg: str) -> bool:
    low = err_msg.lower()
    capacity_keywords = [
        "out of host capacity",
        "outofcapacity",
        "out of capacity",
        "499",
        "500",
        "internal error",
        "toomanyrequests",
        "service unavailable",
    ]
    return any(k in low for k in capacity_keywords)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Repeatedly attempts to launch an OCI Always Free Ampere A1 VM until capacity is available."
    )
    parser.add_argument(
        "--compartment-id",
        required=True,
        help="Compartment OCID (for root compartment 'khuzaimahassanwork', this is your Tenancy OCID).",
    )
    parser.add_argument(
        "--subnet-id",
        required=False,
        help="Subnet OCID for the primary VNIC. If omitted, will auto-detect from compartment.",
    )
    parser.add_argument(
        "--ssh-key-file",
        required=True,
        help="Path to public SSH key file (e.g. ~/.ssh/id_rsa.pub or ~/.ssh/id_ed25519.pub).",
    )
    parser.add_argument(
        "--image-id",
        required=False,
        help="Canonical Ubuntu ARM64 image OCID. If omitted, auto-fetches the latest.",
    )
    parser.add_argument(
        "--ocpus",
        type=int,
        default=2,
        help="Number of OCPUs (default: 2 for Always Free Ampere).",
    )
    parser.add_argument(
        "--memory-gb",
        type=int,
        default=12,
        help="RAM allocation in GB (default: 12).",
    )
    parser.add_argument(
        "--boot-volume-gb",
        type=int,
        default=100,
        help="Boot volume size in GB (default: 100).",
    )
    parser.add_argument(
        "--display-name",
        default="logistics-ampere-a1",
        help="Instance display name.",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Polling retry interval in seconds (default: 60).",
    )
    return parser.parse_args()


def main():
    check_oci_cli()
    args = parse_args()

    ssh_key = Path(os.path.expanduser(args.ssh_key_file)).resolve()
    if not ssh_key.exists():
        log(f"ERROR: SSH public key file not found at: {ssh_key}")
        sys.exit(1)

    compartment_id = args.compartment_id

    # Auto-detect or validate subnet
    subnet_id = args.subnet_id
    if not subnet_id:
        log("Querying subnets in compartment...")
        subnets = get_subnets(compartment_id)
        if not subnets:
            log(
                "ERROR: No subnets found in compartment. Please create a VCN and public subnet, or specify --subnet-id."
            )
            sys.exit(1)
        if len(subnets) == 1:
            subnet_id = subnets[0]["id"]
            log(
                f"Auto-selected only available subnet: {subnets[0].get('display-name')} ({subnet_id})"
            )
        else:
            log("Multiple subnets found:")
            for s in subnets:
                print(f"  - {s.get('display-name')}: {s['id']}")
            subnet_id = subnets[0]["id"]
            log(
                f"Defaulting to first subnet: {subnets[0].get('display-name')} ({subnet_id}). Pass --subnet-id to override."
            )

    # Image ID resolution
    image_id = args.image_id
    if not image_id:
        image_id = get_latest_ubuntu_arm_image(compartment_id)
        if not image_id:
            log(
                "ERROR: Could not resolve a Canonical Ubuntu ARM64 image OCID. Please pass --image-id explicitly."
            )
            sys.exit(1)

    # Availability Domains
    ads = get_availability_domains(compartment_id)
    if not ads:
        log("No availability domains found via API. Using default AD-1.")
        ads = [""]

    log("=" * 65)
    log("Starting OCI Ampere A1 Auto-Provisioning Loop")
    log(f"Compartment:   {compartment_id}")
    log(
        f"Shape:         VM.Standard.A1.Flex ({args.ocpus} OCPUs, {args.memory_gb} GB RAM)"
    )
    log(f"Boot Volume:   {args.boot_volume_gb} GB")
    log(f"Subnet:        {subnet_id}")
    log(f"SSH Key:       {ssh_key}")
    log(f"Retry Window:  Every {args.interval}s")
    log("=" * 65)

    attempt = 1
    while True:
        for ad in ads:
            ad_label = ad if ad else "default"
            log(f"[Attempt {attempt}] Trying Availability Domain: {ad_label}...")

            success, result = attempt_launch(
                compartment_id=compartment_id,
                availability_domain=ad,
                image_id=image_id,
                subnet_id=subnet_id,
                ssh_key_path=str(ssh_key),
                ocpus=args.ocpus,
                memory_in_gbs=args.memory_gb,
                boot_volume_size_in_gbs=args.boot_volume_gb,
                display_name=args.display_name,
            )

            if success:
                log("🎉 SUCCESS! Instance provisioned successfully!")
                instance_data = (
                    result.get("data", {}) if isinstance(result, dict) else {}
                )
                instance_id = instance_data.get("id", "Unknown")
                log(f"Instance OCID: {instance_id}")
                log(f"Display Name:  {instance_data.get('display-name')}")
                log(f"Lifecycle:     {instance_data.get('lifecycle-state')}")

                # Attempt to query public IP
                time.sleep(10)
                vnic_code, vnic_out = run_oci_json(
                    ["compute", "instance", "list-vnics", "--instance-id", instance_id]
                )
                if vnic_code == 0 and isinstance(vnic_out, dict):
                    vnics = vnic_out.get("data", [])
                    if vnics:
                        public_ip = vnics[0].get("public-ip", "Assigning...")
                        log(f"Public IPv4 Address: {public_ip}")
                        log(f"SSH Command: ssh ubuntu@{public_ip}")

                log("Auto-provisioner finished successfully.")
                return

            error_msg = str(result)
            if is_capacity_error(error_msg):
                log(
                    f"Capacity unavailable in {ad_label} (Out of host capacity). Continuing retry loop."
                )
            else:
                log(f"Encountered non-capacity error on {ad_label}: {error_msg}")
                # If error is a fundamental config flaw (e.g., auth failure), stop early
                if (
                    "notauthorized" in error_msg.lower()
                    or "invalidparameter" in error_msg.lower()
                ):
                    log(
                        "FATAL: Authorization or configuration parameter error. Aborting."
                    )
                    sys.exit(1)

        attempt += 1
        log(f"Waiting {args.interval}s before next cycle...")
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
