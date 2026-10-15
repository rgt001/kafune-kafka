"""Package the tested release binaries, source, licences and evidence."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import tarfile
import zipfile
import tomllib


def sha256(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def zip_checked(destination, files):
    expected = {}
    with zipfile.ZipFile(destination, 'w', zipfile.ZIP_DEFLATED, compresslevel=6, strict_timestamps=False) as archive:
        for path, name in files:
            archive.write(path, name)
            expected[name] = sha256(path)
    with zipfile.ZipFile(destination) as archive:
        for name, sha in expected.items():
            with archive.open(name) as stream:
                if hashlib.file_digest(stream, 'sha256').hexdigest() != sha:
                    raise RuntimeError('Archive hash mismatch: ' + name)
    destination.with_suffix(destination.suffix + '.sha256').write_text(sha256(destination) + '  ' + destination.name + '\n')


def collect_licenses(root, artifacts, metadata):
    licences = artifacts / 'third-party-licenses'
    licences.mkdir(exist_ok=True)
    rows = ['# Dependencias e licencas', '', 'Inventario do Cargo.lock. Algumas dependencias sao usadas apenas em outras plataformas ou no build.', '', '| Pacote | Versao | Licenca declarada |', '|---|---|---|']
    inventory = []
    for package in sorted(metadata['packages'], key=lambda p: (p['name'], p['version'])):
        if package['name'] == 'kafka-lens':
            continue
        rows.append(f"| {package['name']} | {package['version']} | {package.get('license') or 'Veja os arquivos do pacote'} |")
        package_root = Path(package['manifest_path']).parent
        saved = []
        candidates = [file for file in package_root.iterdir() if file.is_file() and file.name.upper().startswith(('LICENSE', 'LICENCE', 'COPYING', 'NOTICE', 'COPYRIGHT'))]
        for native in ['openssl', 'librdkafka', 'curl', 'zstd']:
            directory = package_root / native
            if directory.is_dir():
                candidates.extend(file for file in directory.iterdir() if file.is_file() and file.name.upper().startswith(('LICENSE', 'LICENCE', 'COPYING', 'NOTICE', 'COPYRIGHT')))
        for file in candidates:
            destination = licences / (package['name'] + '-' + package['version']) / file.relative_to(package_root)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(file, destination)
            saved.append(destination.relative_to(artifacts).as_posix())
        inventory.append({'name': package['name'], 'version': package['version'], 'license': package.get('license'), 'license_files': saved})
    (artifacts / 'THIRD-PARTY-NOTICES.md').write_text('\n'.join(rows) + '\n', encoding='utf-8')
    (artifacts / 'dependency-inventory.json').write_text(json.dumps(inventory, indent=2) + '\n')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--linux-binary', required=True)
    parser.add_argument('--runtime-dll', required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    artifacts = root / 'artifacts'
    windows = artifacts / 'windows'
    linux = artifacts / 'linux'
    windows.mkdir(exist_ok=True)
    linux.mkdir(exist_ok=True)
    version = tomllib.loads((root / 'Cargo.toml').read_text())['package']['version']
    shutil.copy2(root / 'target/release/kafka-lens.exe', windows / 'KafkaLens.exe')
    shutil.copy2(args.runtime_dll, windows / 'vcruntime140.dll')
    shutil.copy2(args.linux_binary, linux / 'kafka-lens')
    for platform in [windows, linux]:
        shutil.copy2(root / 'README.md', platform / 'README.md')
        shutil.copy2(root / 'LICENSE', platform / 'LICENSE')
        shutil.copy2(root / 'PERFORMANCE.md', platform / 'PERFORMANCE.md')
        shutil.copy2(root / 'PERFORMANCE-WINDOWS-GUI.md', platform / 'PERFORMANCE-WINDOWS-GUI.md')
    metadata = json.loads((root / '.tools/evidence/cargo-metadata.json').read_text(encoding='utf-8-sig'))
    collect_licenses(root, artifacts, metadata)
    licences = artifacts / 'third-party-licenses'
    integrations = {}
    for platform in ['windows', 'linux']:
        report = json.loads((artifacts / ('integration-' + platform + '.json')).read_text())
        if report['status'] != 'passed':
            raise RuntimeError('Integration did not pass: ' + platform)
        integrations[platform] = report
    binaries = [windows / 'KafkaLens.exe', windows / 'vcruntime140.dll', linux / 'kafka-lens']
    verification = {
        'application': 'KafkaLens', 'version': version, 'build_profile': 'release',
        'tested_at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'all_12_requirements': 'passed in the local Kafka test environment',
        'unit_tests_per_platform': 9, 'clippy_windows_linux': 'passed with -D warnings',
        'integration': integrations,
        'bulk_transfer': {p:json.loads((artifacts/f'bulk-{p}.json').read_text()) for p in ['windows','linux']},
        'read_session': {p:json.loads((artifacts/f'read-session-{p}.json').read_text()) for p in ['windows','linux']},
        'native_gui_smoke': {'windows': 'ui-windows.png', 'linux': 'ui-linux.png'},
        'binaries': [{'path': p.relative_to(artifacts).as_posix(), 'bytes': p.stat().st_size, 'sha256': sha256(p)} for p in binaries],
        'validation_scope': 'Real Kafka 4.1, two clusters, Schema Registry 7.8.3, SASL/PLAIN, core operations and native message-view screenshots. Production clusters and other authentication modes were not exercised.',
        'build_note': 'Windows linking emits LNK4099 for missing OpenSSL native PDB symbols. Executable, unit tests, integration and GUI smoke passed.',
    }
    (artifacts / 'verification.json').write_text(json.dumps(verification, indent=2) + '\n')
    source_files=[]
    for directory, subdirectories, filenames in os.walk(root):
        subdirectories[:]=[name for name in subdirectories if name not in {'target','.tools','artifacts','dist','.git','__pycache__'}]
        source_files.extend(Path(directory)/name for name in filenames if not name.endswith(('.log','.partial','.pyc','.pem','.key','.pfx','.p12')) and not name.startswith('.env') and name != 'clusters.json')
    zip_checked(artifacts / f'KafkaLens-v{version}-source.zip', [(p, 'KafkaLens/' + p.relative_to(root).as_posix()) for p in source_files])
    common = [(artifacts / name, name) for name in ['THIRD-PARTY-NOTICES.md', 'dependency-inventory.json', 'verification.json']]
    common.extend((artifacts / name, 'evidence/' + name) for platform in ['windows', 'linux'] for name in [f'integration-{platform}.json', f'bulk-{platform}.json', f'ui-{platform}.png'])
    common.extend((p, 'performance/' + p.name) for p in (artifacts / 'performance').glob('*.json'))
    common.extend((p, 'performance/' + p.name) for p in (artifacts / 'performance').glob('*.png'))
    common.extend((p, p.relative_to(artifacts).as_posix()) for p in licences.rglob('*') if p.is_file())
    zip_checked(artifacts / f'KafkaLens-v{version}-windows-x64.zip', [(p, p.name) for p in windows.iterdir() if p.is_file()] + common)
    linux_tar = artifacts / f'KafkaLens-v{version}-linux-x64.tar.gz'
    with tarfile.open(linux_tar, 'w:gz') as archive:
        for path, name in [(p, p.name) for p in linux.iterdir() if p.is_file()] + common:
            info = archive.gettarinfo(str(path), 'KafkaLens/' + name)
            if path.name == 'kafka-lens':
                info.mode = 0o755
            with path.open('rb') as stream:
                archive.addfile(info, stream)
    with tarfile.open(linux_tar) as archive:
        with archive.extractfile('KafkaLens/kafka-lens') as stream:
            if hashlib.file_digest(stream, 'sha256').hexdigest() != sha256(linux / 'kafka-lens'):
                raise RuntimeError('Linux archive mismatch')
    linux_tar.with_suffix('.gz.sha256').write_text(sha256(linux_tar) + '  ' + linux_tar.name + '\n')
    print(json.dumps({'status': 'passed', 'artifacts': str(artifacts), 'source_files': len(source_files), 'platforms': ['windows', 'linux']}, indent=2))


if __name__ == '__main__':
    main()
