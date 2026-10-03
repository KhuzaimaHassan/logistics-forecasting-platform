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


OCI_BIN = None


def get_oci_binary() -> str:
    global OCI_BIN
    if OCI_BIN:
        return OCI_BIN
    path = shutil.which("oci")
    if path:
        OCI_BIN = path
        return OCI_BIN
    # Check ~/.local/bin on Windows / Linux
    local_bin = Path(os.path.expanduser("~/.local/bin/oci.exe"))
    if local_bin.exists():
        OCI_BIN = str(local_bin)
        return OCI_BIN
    local_bin_noext = Path(os.path.expanduser("~/.local/bin/oci"))
    if local_bin_noext.exists():
        OCI_BIN = str(local_bin_noext)
        return OCI_BIN
    log("ERROR: OCI CLI ('oci') not found in PATH or ~/.local/bin.")
    log("Please install the OCI CLI first. See setup instructions.")
    sys.exit(1)


def check_oci_cli():
    get_oci_binary()


def run_oci_json(args: list[str]) -> tuple[int, dict | str]:
    cmd = [get_oci_binary()] + args + ["--output", "json"]
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


def ensure_network_infrastructure(compartment_id: str) -> str:
    log("Checking for existing VCN and subnets...")
    subnets = get_subnets(compartment_id)
    if subnets:
        if len(subnets) == 1:
            log(
                f"Auto-selected available subnet: {subnets[0].get('display-name')} ({subnets[0]['id']})"
            )
            return subnets[0]["id"]
        log("Multiple subnets found in compartment:")
        for s in subnets:
            print(f"  - {s.get('display-name')}: {s['id']}")
        log(f"Defaulting to: {subnets[0].get('display-name')} ({subnets[0]['id']})")
        return subnets[0]["id"]

    log(
        "No subnets found. Automatically provisioning standard Always Free VCN and Public Subnet..."
    )
    # 1. Create VCN
    log("Creating VCN 'logistics-vcn' (10.0.0.0/16)...")
    code, vcn_res = run_oci_json(
        [
            "network",
            "vcn",
            "create",
            "--compartment-id",
            compartment_id,
            "--cidr-block",
            "10.0.0.0/16",
            "--display-name",
            "logistics-vcn",
            "--dns-label",
            "logistics",
        ]
    )
    if code != 0 or not isinstance(vcn_res, dict):
        log(f"ERROR creating VCN: {vcn_res}")
        sys.exit(1)
    vcn_id = vcn_res["data"]["id"]
    default_rt_id = vcn_res["data"]["default-route-table-id"]
    default_sl_id = vcn_res["data"]["default-security-list-id"]
    log(f"Created VCN: {vcn_id}")

    # 2. Create Internet Gateway
    log("Creating Internet Gateway 'logistics-igw'...")
    code, igw_res = run_oci_json(
        [
            "network",
            "internet-gateway",
            "create",
            "--compartment-id",
            compartment_id,
            "--vcn-id",
            vcn_id,
            "--is-enabled",
            "true",
            "--display-name",
            "logistics-igw",
        ]
    )
    if code != 0 or not isinstance(igw_res, dict):
        log(f"ERROR creating Internet Gateway: {igw_res}")
        sys.exit(1)
    igw_id = igw_res["data"]["id"]
    log(f"Created Internet Gateway: {igw_id}")

    # 3. Add default route (0.0.0.0/0 -> IGW)
    log("Configuring default route to Internet Gateway...")
    route_rules = json.dumps([{"cidrBlock": "0.0.0.0/0", "networkEntityId": igw_id}])
    run_oci_json(
        [
            "network",
            "route-table",
            "update",
            "--rt-id",
            default_rt_id,
            "--route-rules",
            route_rules,
            "--force",
        ]
    )

    # 4. Configure Security List for ports 22, 80, 443
    log("Updating Security List for ports 22 (SSH), 80 (HTTP), 443 (HTTPS)...")
    ingress_rules = json.dumps(
        [
            {
                "protocol": "6",
                "source": "0.0.0.0/0",
                "tcpOptions": {"destinationPortRange": {"min": 22, "max": 22}},
                "description": "SSH",
            },
            {
                "protocol": "6",
                "source": "0.0.0.0/0",
                "tcpOptions": {"destinationPortRange": {"min": 80, "max": 80}},
                "description": "HTTP (Caddy TLS)",
            },
            {
                "protocol": "6",
                "source": "0.0.0.0/0",
                "tcpOptions": {"destinationPortRange": {"min": 443, "max": 443}},
                "description": "HTTPS (Caddy)",
            },
        ]
    )
    run_oci_json(
        [
            "network",
            "security-list",
            "update",
            "--security-list-id",
            default_sl_id,
            "--ingress-security-rules",
            ingress_rules,
            "--force",
        ]
    )

    # 5. Create Public Subnet
    log("Creating Public Subnet 'logistics-public-subnet' (10.0.0.0/24)...")
    code, subnet_res = run_oci_json(
        [
            "network",
            "subnet",
            "create",
            "--compartment-id",
            compartment_id,
            "--vcn-id",
            vcn_id,
            "--cidr-block",
            "10.0.0.0/24",
            "--display-name",
            "logistics-public-subnet",
            "--dns-label",
            "public",
        ]
    )
    if code != 0 or not isinstance(subnet_res, dict):
        log(f"ERROR creating Subnet: {subnet_res}")
        sys.exit(1)
    subnet_id = subnet_res["data"]["id"]
    log(f"Created Public Subnet: {subnet_id}")
    return subnet_id


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
        get_oci_binary(),
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
        help="Subnet OCID for the primary VNIC. If omitted, will auto-detect or create.",
    )
    parser.add_argument(
        "--ssh-key-file",
        required=False,
        default=None,
        help="Path to public SSH key file (e.g. ~/.ssh/id_rsa.pub or ~/.ssh/id_ed25519.pub). If omitted, auto-detects from ~/.ssh.",
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

    if args.ssh_key_file:
        ssh_key = Path(os.path.expanduser(args.ssh_key_file)).resolve()
    else:
        candidates = [
            Path(os.path.expanduser("~/.ssh/id_ed25519.pub")),
            Path(os.path.expanduser("~/.ssh/id_rsa.pub")),
            Path(os.path.expanduser("~/.ssh/id_ecdsa.pub")),
        ]
        ssh_key = next((c for c in candidates if c.exists()), None)
        if not ssh_key:
            log(
                "ERROR: No SSH public key found in ~/.ssh/ (checked id_ed25519.pub, id_rsa.pub)."
            )
            log("Please specify --ssh-key-file <path_to_key.pub> explicitly.")
            sys.exit(1)
        log(f"Auto-detected SSH public key: {ssh_key}")

    if not ssh_key.exists():
        log(f"ERROR: SSH public key file not found at: {ssh_key}")
        sys.exit(1)

    compartment_id = args.compartment_id

    # Auto-detect or provision subnet
    subnet_id = args.subnet_id
    if not subnet_id:
        subnet_id = ensure_network_infrastructure(compartment_id)

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

                # Query assigned public IP
                time.sleep(10)
                vnic_code, vnic_out = run_oci_json(
                    [
                        "compute",
                        "instance",
                        "list-vnics",
                        "--instance-id",
                        instance_id,
                    ]
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
