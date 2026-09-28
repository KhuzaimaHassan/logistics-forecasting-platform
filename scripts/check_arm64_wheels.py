"""Script to verify ARM64 (aarch64) wheel availability for key packages."""

import json
import urllib.request

PACKAGES = [
    "lightgbm",
    "faiss-cpu",
    "fastembed",
    "onnxruntime",
    "evidently",
    "shapely",
    "psycopg-binary",
    "psycopg2-binary",
]


def check_package_wheels(pkg_name: str):
    url = f"https://pypi.org/pypi/{pkg_name}/json"
    req = urllib.request.Request(
        url, headers={"User-Agent": "arm64-compatibility-checker"}
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
    except Exception as e:
        return {"package": pkg_name, "error": str(e)}

    version = data["info"]["version"]
    urls = data["urls"]

    arm64_wheels = []
    any_wheels = []
    for u in urls:
        filename = u["filename"]
        if not filename.endswith(".whl"):
            continue
        if "aarch64" in filename or "arm64" in filename:
            arm64_wheels.append(filename)
        elif "any.whl" in filename or "py3-none-any" in filename:
            any_wheels.append(filename)

    return {
        "package": pkg_name,
        "latest_version": version,
        "arm64_wheels": arm64_wheels,
        "pure_python_wheels": any_wheels,
        "total_files": len(urls),
    }


def main():
    print(
        f"{'Package':<18} | {'Version':<10} | {'ARM64 Wheel':<15} | {'Pure Python':<15}"
    )
    print("-" * 65)
    for pkg in PACKAGES:
        res = check_package_wheels(pkg)
        if "error" in res:
            print(f"{pkg:<18} | ERROR: {res['error']}")
            continue
        has_arm = "YES" if res["arm64_wheels"] else "NO"
        has_pure = "YES" if res["pure_python_wheels"] else "NO"
        print(
            f"{res['package']:<18} | {res['latest_version']:<10} | {has_arm:<15} | {has_pure:<15}"
        )
        if res["arm64_wheels"]:
            for w in res["arm64_wheels"][:2]:
                print(f"   -> {w}")
        if res["pure_python_wheels"]:
            for w in res["pure_python_wheels"][:2]:
                print(f"   -> {w}")


if __name__ == "__main__":
    main()
