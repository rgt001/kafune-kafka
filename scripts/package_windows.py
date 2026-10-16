"""Create a portable Windows release from a built executable; no local test data."""
import argparse
import datetime
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import tempfile
import tomllib
from package import collect_licenses, sha256, zip_checked

ROOT = Path(__file__).resolve().parents[1]


def runtime_dlls(explicit):
    if explicit:
        path = explicit.resolve(strict=True)
        candidates = sorted(path.parent.glob("vcruntime140*.dll"))
    else:
        vswhere = Path(os.environ.get("ProgramFiles(x86)", "C:/Program Files (x86)")) / "Microsoft Visual Studio/Installer/vswhere.exe"
        installation = subprocess.check_output([str(vswhere), "-latest", "-products", "*", "-requires",
                                                "Microsoft.VisualStudio.Component.VC.Tools.x86.x64",
                                                "-property", "installationPath"], text=True).strip()
        if not installation:
            raise RuntimeError("MSVC not found; pass --runtime-dll with the x64 vcruntime140.dll path")
        paths = sorted((p for p in (Path(installation) / "VC/Redist/MSVC").glob("*/x64/Microsoft.VC*.CRT/vcruntime140.dll")
                        if all(n.isdigit() for n in p.parents[2].name.split("."))),
                       key=lambda p: tuple(int(n) for n in p.parents[2].name.split(".")))
        if not paths:
            raise RuntimeError("MSVC x64 runtime not found; pass --runtime-dll")
        candidates = sorted(paths[-1].parent.glob("vcruntime140*.dll"))
    if not any(p.name.lower() == "vcruntime140.dll" for p in candidates):
        raise RuntimeError("Missing vcruntime140.dll")
    return candidates


def require_x64_pe(path):
    with path.open("rb") as stream:
        if stream.read(2) != b"MZ":
            raise ValueError(f"Not a Windows executable: {path.name}")
        stream.seek(0x3C)
        stream.seek(struct.unpack("<I", stream.read(4))[0])
        if stream.read(4) != b"PE\0\0" or struct.unpack("<H", stream.read(2))[0] != 0x8664:
            raise ValueError(f"Expected Windows x64 PE: {path.name}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, default=ROOT / "target/release/kafunekafka.exe")
    parser.add_argument("--runtime-dll", type=Path)
    parser.add_argument("--metadata", type=Path, help="Existing cargo metadata JSON, otherwise generated with --locked")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "dist")
    parser.add_argument("--tag", help="If supplied, must match v<version from Cargo.toml>")
    args = parser.parse_args()
    version = tomllib.loads((ROOT / "Cargo.toml").read_text())["package"]["version"]
    if args.tag and args.tag != f"v{version}":
        parser.error(f"Tag {args.tag!r} must equal v{version}")
    require_x64_pe(args.binary)
    dlls = runtime_dlls(args.runtime_dll)
    for dll in dlls:
        require_x64_pe(dll)
    metadata = json.loads(args.metadata.read_text(encoding="utf-8-sig")) if args.metadata else json.loads(
        subprocess.check_output(["cargo", "metadata", "--locked", "--format-version", "1"], cwd=ROOT))
    args.output_dir.mkdir(parents=True, exist_ok=True)
    destination = args.output_dir / f"kafunekafka-v{version}-windows-x64.zip"
    with tempfile.TemporaryDirectory(prefix="kafunekafka-release-") as temporary:
        folder = Path(temporary)
        shutil.copy2(args.binary, folder / "kafunekafka.exe")
        for dll in dlls:
            shutil.copy2(dll, folder / dll.name)
        for name in ["README.md", "README.pt-BR.md", "RELEASE-NOTES.md", "LICENSE", "PERFORMANCE.md", "PERFORMANCE-WINDOWS-GUI.md"]:
            shutil.copy2(ROOT / name, folder / name)
        shutil.copytree(ROOT / "assets", folder / "assets")
        collect_licenses(ROOT, folder, metadata)
        manifest = {"application": "kafunekafka", "version": version, "platform": "windows-x64",
                    "packaged_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    "binary_sha256": sha256(folder / "kafunekafka.exe"),
                    "files": [{"name": p.name, "sha256": sha256(p)} for p in folder.iterdir() if p.suffix in {".exe", ".dll"}],
                    "contents": "Portable executable, MSVC runtime, documentation and dependency licences. No cluster profiles or local benchmark data."}
        (folder / "release-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        files = [(path, path.relative_to(folder).as_posix()) for path in sorted(folder.rglob("*")) if path.is_file()]
        zip_checked(destination, files)
    print(json.dumps({"status": "passed", "archive": str(destination), "bytes": destination.stat().st_size,
                      "sha256": sha256(destination), "binary_sha256": manifest["binary_sha256"]}, indent=2))


if __name__ == "__main__":
    main()
