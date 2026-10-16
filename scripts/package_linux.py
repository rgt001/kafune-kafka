"""Package an already built Linux x64 executable, documentation and licences."""
import argparse
import datetime
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import tomllib

from package import collect_licenses, sha256

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, default=ROOT / "target/release/kafunekafka")
    parser.add_argument("--metadata", type=Path)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "dist")
    parser.add_argument("--tag")
    args = parser.parse_args()
    version = tomllib.loads((ROOT / "Cargo.toml").read_text())["package"]["version"]
    if args.tag and args.tag != f"v{version}":
        parser.error(f"Tag must equal v{version}")
    with args.binary.open("rb") as stream:
        header = stream.read(20)
    if header[:6] != b"\x7fELF\x02\x01" or header[18:20] != b"\x3e\x00":
        parser.error("Expected a Linux x64 ELF executable")
    metadata = json.loads(args.metadata.read_text(encoding="utf-8-sig")) if args.metadata else json.loads(
        subprocess.check_output(["cargo", "metadata", "--locked", "--format-version", "1"], cwd=ROOT))
    args.output_dir.mkdir(parents=True, exist_ok=True)
    destination = args.output_dir / f"kafunekafka-v{version}-linux-x64.tar.gz"
    with tempfile.TemporaryDirectory(prefix="kafunekafka-linux-") as temporary:
        folder = Path(temporary)
        shutil.copy2(args.binary, folder / "kafunekafka")
        for name in ["README.md", "README.pt-BR.md", "README.ru.md", "RELEASE-NOTES.md", "LICENSE", "PERFORMANCE.md", "PERFORMANCE-WINDOWS-GUI.md"]:
            shutil.copy2(ROOT / name, folder / name)
        shutil.copytree(ROOT / "assets", folder / "assets")
        shutil.copytree(ROOT / "locales", folder / "locales")
        collect_licenses(ROOT, folder, metadata)
        manifest = {"application": "kafunekafka", "version": version, "platform": "linux-x64",
                    "packaged_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    "binary_sha256": sha256(folder / "kafunekafka"),
                    "runtime": "Built on Ubuntu 22.04 (glibc 2.35); requires an X11/Wayland session and OpenGL."}
        (folder / "release-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        expected = {}
        with tarfile.open(destination, "w:gz") as archive:
            for path in sorted(folder.rglob("*")):
                if not path.is_file():
                    continue
                name = "kafunekafka/" + path.relative_to(folder).as_posix()
                info = archive.gettarinfo(str(path), name)
                info.uid = info.gid = 0
                info.uname = info.gname = ""
                info.mode = 0o755 if path.name == "kafunekafka" else 0o644
                with path.open("rb") as stream:
                    archive.addfile(info, stream)
                expected[name] = sha256(path)
        with tarfile.open(destination, "r:gz") as archive:
            for name, digest in expected.items():
                with archive.extractfile(name) as stream:
                    if hashlib.file_digest(stream, "sha256").hexdigest() != digest:
                        raise RuntimeError("Archive hash mismatch: " + name)
        destination.with_suffix(".gz.sha256").write_text(sha256(destination) + "  " + destination.name + "\n")
    print(json.dumps({"status": "passed", "archive": str(destination), "bytes": destination.stat().st_size,
                      "sha256": sha256(destination), "binary_sha256": manifest["binary_sha256"]}, indent=2))


if __name__ == "__main__":
    main()
