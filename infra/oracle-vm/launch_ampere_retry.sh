#!/usr/bin/env bash
# Automated OCI Always Free Ampere A1 Instance Provisioning Script with Retry Loop.
# Catches "Out of host capacity" (499/500) and loops every 60 seconds until successfully provisioned.

set -euo pipefail

# Configuration Parameters (override via environment variables or edit below)
COMPARTMENT_ID="${COMPARTMENT_ID:?Error: COMPARTMENT_ID must be set (for root compartment, this is your Tenancy OCID)}"
SUBNET_ID="${SUBNET_ID:?Error: SUBNET_ID must be set (public subnet OCID in your VCN)}"
SSH_KEY_FILE="${SSH_KEY_FILE:-$HOME/.ssh/id_rsa.pub}"
SHAPE="VM.Standard.A1.Flex"
OCPUS=2
MEMORY_GBS=12
BOOT_VOLUME_GBS=100
DISPLAY_NAME="${DISPLAY_NAME:-logistics-ampere-a1}"
INTERVAL="${INTERVAL:-60}"

if ! command -v oci &> /dev/null; then
    echo "ERROR: OCI CLI ('oci') not found in PATH. Please install it first."
    exit 1
fi

if [ ! -f "$SSH_KEY_FILE" ]; then
    echo "ERROR: Public SSH key file not found at: $SSH_KEY_FILE"
    exit 1
fi

echo "===> Resolving latest Canonical Ubuntu LTS ARM64 Image..."
IMAGE_ID="${IMAGE_ID:-$(oci compute image list \
    --compartment-id "$COMPARTMENT_ID" \
    --operating-system "Canonical Ubuntu" \
    --shape "$SHAPE" \
    --sort-by TIMECREATED \
    --sort-order DESC \
    --query 'data[?contains("display-name", `aarch64`) || contains("display-name", `arm64`)].id | [0]' \
    --raw-output 2>/dev/null || true)}"

if [ -z "$IMAGE_ID" ] || [ "$IMAGE_ID" = "null" ]; then
    echo "Fallback: Querying latest Canonical Ubuntu image..."
    IMAGE_ID=$(oci compute image list \
        --compartment-id "$COMPARTMENT_ID" \
        --operating-system "Canonical Ubuntu" \
        --shape "$SHAPE" \
        --sort-by TIMECREATED \
        --sort-order DESC \
        --query 'data[0].id' \
        --raw-output)
fi
echo "===> Using Image OCID: $IMAGE_ID"

echo "===> Fetching Availability Domains for compartment..."
readarray -t ADS < <(oci iam availability-domain list --compartment-id "$COMPARTMENT_ID" --query 'data[*].name' --raw-output)
echo "===> Found ${#ADS[@]} Availability Domain(s): ${ADS[*]}"

echo "================================================================="
echo " Starting Ampere A1 Provisioning Retry Loop"
echo " Shape:         $SHAPE ($OCPUS OCPUs, ${MEMORY_GBS}GB RAM, ${BOOT_VOLUME_GBS}GB Boot Disk)"
echo " Subnet:        $SUBNET_ID"
echo " Retry Window:  Every ${INTERVAL}s"
echo "================================================================="

SHAPE_CONFIG="{\"ocpus\":$OCPUS,\"memoryInGBs\":$MEMORY_GBS}"
ATTEMPT=1

while true; do
    for AD in "${ADS[@]}"; do
        TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
        echo "[$TIMESTAMP] [Attempt $ATTEMPT] Requesting capacity in AD: $AD..."

        # Execute instance launch command capturing output and exit code
        if OUTPUT=$(oci compute instance launch \
            --compartment-id "$COMPARTMENT_ID" \
            --availability-domain "$AD" \
            --shape "$SHAPE" \
            --shape-config "$SHAPE_CONFIG" \
            --image-id "$IMAGE_ID" \
            --subnet-id "$SUBNET_ID" \
            --assign-public-ip true \
            --boot-volume-size-in-gbs "$BOOT_VOLUME_GBS" \
            --ssh-authorized-keys-file "$SSH_KEY_FILE" \
            --display-name "$DISPLAY_NAME" \
            --output json 2>&1); then

            echo "================================================================="
            echo "🎉 SUCCESS! Instance provisioned successfully!"
            INSTANCE_ID=$(echo "$OUTPUT" | oci --output raw-output --query 'data.id')
            echo "Instance OCID: $INSTANCE_ID"
            echo "Waiting for Public IP assignment..."
            sleep 15

            PUBLIC_IP=$(oci compute instance list-vnics --instance-id "$INSTANCE_ID" \
                --query 'data[0]."public-ip"' --raw-output 2>/dev/null || echo "Pending...")
            echo "Public IPv4:   $PUBLIC_IP"
            echo "SSH Command:   ssh ubuntu@$PUBLIC_IP"
            echo "================================================================="
            exit 0
        else
            # Check for Out of Capacity or transient 499/500 errors
            if echo "$OUTPUT" | grep -Ei "out of host capacity|outofcapacity|499|500|toomanyrequests|internal error" > /dev/null; then
                echo "[$TIMESTAMP] [AD: $AD] Out of host capacity. Retrying next AD..."
            else
                echo "[$TIMESTAMP] Unexpected error on AD $AD:"
                echo "$OUTPUT"
                # If error is fatal authorization or parameter issue, exit
                if echo "$OUTPUT" | grep -Ei "notauthorized|invalidparameter" > /dev/null; then
                    echo "FATAL: Authentication or invalid parameter error. Exiting."
                    exit 1
                fi
            fi
        fi
    done

    ATTEMPT=$((ATTEMPT + 1))
    echo "Capacity currently full across all ADs. Sleeping ${INTERVAL}s before next attempt..."
    sleep "$INTERVAL"
done
