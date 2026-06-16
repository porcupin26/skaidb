# Installing skaidb

Every push to `main` publishes a versioned [GitHub Release](https://github.com/porcupin26/skaidb/releases)
with prebuilt binaries and packages for Linux, macOS, and Windows, plus a
`SHA256SUMS` file. You can also build from source. Every bundle ships **two
binaries**:

- `skaidb` — the database **server** (binary + REST endpoints, clustering).
- `skaidbsh` — the unified **shell and admin client**: an interactive SQL shell
  that connects over the network (with nearest-node selection and failover),
  plus cluster membership and configuration commands (`cluster`, `config`,
  `status`, `metrics`) — see [CLUSTERING.md](CLUSTERING.md). It can also open a
  data directory directly with `--local` for offline use.

> Replace `X.Y.Z` below with the release you want (e.g. `0.3.0`), or use the
> latest-version snippet in [Downloading the right file](#downloading-the-right-file).

## Contents

- [Which download do I want?](#which-download-do-i-want)
- [Downloading the right file](#downloading-the-right-file)
- [Verifying the download](#verifying-the-download-recommended)
- [Linux](#linux)
  - [Debian / Ubuntu (.deb)](#debian--ubuntu-deb)
  - [Fedora / RHEL / CentOS / Rocky / Alma (.rpm)](#fedora--rhel--centos--rocky--alma-rpm)
  - [openSUSE (.rpm)](#opensuse-rpm)
  - [Any distro — tarball (glibc)](#any-distro--tarball-glibc)
  - [Any distro / Alpine — static musl tarball](#any-distro--alpine--static-musl-tarball)
- [macOS](#macos)
  - [Disk image (.dmg)](#disk-image-dmg)
  - [Tarball](#tarball-macos)
- [Windows](#windows)
  - [Zip archive](#zip-archive)
  - [Standalone .exe](#standalone-exe)
- [Build from source](#build-from-source)
- [Binary-only (no package, no build)](#binary-only-no-package-no-build)
- [Run it / verify the install](#run-it--verify-the-install)
- [Upgrading](#upgrading)
- [Uninstalling](#uninstalling)

## Which download do I want?

| Platform | CPU | Recommended | Also available |
|----------|-----|-------------|----------------|
| Debian, Ubuntu, Mint, … | x86-64 | `skaidb_X.Y.Z_amd64.deb` | tarball |
| Debian, Ubuntu, … | ARM64 | `skaidb_X.Y.Z_arm64.deb` | tarball |
| Fedora, RHEL, Rocky, Alma, openSUSE | x86-64 | `skaidb-X.Y.Z-1.x86_64.rpm` | tarball |
| Fedora, RHEL, … | ARM64 | `skaidb-X.Y.Z-1.aarch64.rpm` | tarball |
| Alpine / "any Linux, no deps" | x86-64 | `skaidb-X.Y.Z-x86_64-unknown-linux-musl.tar.gz` (static) | — |
| Any Linux (glibc) | x86-64 | `skaidb-X.Y.Z-x86_64-unknown-linux-gnu.tar.gz` | .deb/.rpm |
| Any Linux (glibc) | ARM64 | `skaidb-X.Y.Z-aarch64-unknown-linux-gnu.tar.gz` | .deb/.rpm |
| macOS | Apple Silicon (M-series) | `skaidb-X.Y.Z-aarch64-apple-darwin.dmg` | `.tar.gz` |
| macOS | Intel | `skaidb-X.Y.Z-x86_64-apple-darwin.dmg` | `.tar.gz` |
| Windows | x86-64 | `skaidb-X.Y.Z-x86_64-pc-windows-msvc.zip` | standalone `.exe` |

Not sure of your CPU? `uname -m` on Linux/macOS (`x86_64` → amd64/x86_64,
`aarch64`/`arm64` → ARM64). On Apple Silicon Macs `uname -m` prints `arm64`.

## Downloading the right file

Download from the [Releases page](https://github.com/porcupin26/skaidb/releases)
in a browser, or scripted. To always grab the **latest** version:

```sh
# Linux / macOS: resolve the latest tag, then download an asset by name.
REPO=porcupin26/skaidb
VERSION=$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" \
  | grep -oE '"tag_name": *"v[^"]+"' | head -1 | sed -E 's/.*"v([^"]+)".*/\1/')
echo "latest is $VERSION"

# Example: download the glibc x86-64 tarball + the checksums file.
base="https://github.com/$REPO/releases/download/v$VERSION"
curl -fsSLO "$base/skaidb-$VERSION-x86_64-unknown-linux-gnu.tar.gz"
curl -fsSLO "$base/SHA256SUMS"
```

A specific version is just `.../releases/download/vX.Y.Z/<asset>`.

## Verifying the download (recommended)

Each release includes `SHA256SUMS` covering every asset.

```sh
# Linux
sha256sum -c SHA256SUMS --ignore-missing

# macOS
shasum -a 256 -c SHA256SUMS --ignore-missing
```

```powershell
# Windows (PowerShell): compare the printed hash to the matching line in SHA256SUMS
Get-FileHash .\skaidb-X.Y.Z-x86_64-pc-windows-msvc.zip -Algorithm SHA256
Select-String skaidb-X.Y.Z-x86_64-pc-windows-msvc.zip .\SHA256SUMS
```

A `... : OK` line (Linux/macOS) means the file is intact. (Releases are not yet
GPG-signed; verify over HTTPS from the official repo.)

## Linux

The `.deb`/`.rpm` install `skaidb` and `skaidbsh` to `/usr/bin` and drop the
license + README under `/usr/share/doc/skaidb/`.

### Debian / Ubuntu (.deb)

```sh
# x86-64
sudo apt install ./skaidb_X.Y.Z_amd64.deb
# ARM64
sudo apt install ./skaidb_X.Y.Z_arm64.deb
```

`apt install ./file.deb` resolves dependencies. On older systems without that
syntax, use `sudo dpkg -i skaidb_X.Y.Z_amd64.deb` (then `sudo apt -f install` if
it reports missing deps).

### Fedora / RHEL / CentOS / Rocky / Alma (.rpm)

```sh
# x86-64
sudo dnf install ./skaidb-X.Y.Z-1.x86_64.rpm
# ARM64
sudo dnf install ./skaidb-X.Y.Z-1.aarch64.rpm
```

On older systems use `sudo yum install ./skaidb-X.Y.Z-1.x86_64.rpm`, or
`sudo rpm -i skaidb-X.Y.Z-1.x86_64.rpm` for a dependency-free install.

### openSUSE (.rpm)

```sh
sudo zypper install ./skaidb-X.Y.Z-1.x86_64.rpm
```

### Any distro — tarball (glibc)

For distros where you'd rather not use a package, or to install without root:

```sh
tar xzf skaidb-X.Y.Z-x86_64-unknown-linux-gnu.tar.gz   # → skaidb, skaidbsh, LICENSE, README.md
sudo install -m 0755 skaidb skaidbsh /usr/local/bin/
# …or, no root, into your user path:
mkdir -p ~/.local/bin && install -m 0755 skaidb skaidbsh ~/.local/bin/
```

Use the `aarch64-unknown-linux-gnu` tarball on ARM64. The glibc build needs a
reasonably recent glibc; if you hit a `GLIBC_…` error, use the musl build below.

### Any distro / Alpine — static musl tarball

A fully static build with no libc dependency — works on Alpine and anywhere the
glibc build won't:

```sh
tar xzf skaidb-X.Y.Z-x86_64-unknown-linux-musl.tar.gz
sudo install -m 0755 skaidb skaidbsh /usr/local/bin/
```

### Run as a service (systemd)

The `.deb` / `.rpm` install a systemd unit and a default config, and create an
unprivileged `skaidb` system account that owns the data directory:

| Path | Purpose |
|------|---------|
| `/lib/systemd/system/skaidb.service` | the service unit |
| `/etc/skaidb/skaidb.toml` | main configuration (not overwritten on upgrade) |
| `/etc/default/skaidb` | optional `SKAIDB_*` env overrides (win over the file) |
| `/var/lib/skaidb` | data directory, owned by the `skaidb` user |

Installing does **not** auto-start the server, so you can configure it first:

```sh
sudoedit /etc/skaidb/skaidb.toml          # set bind_addr, cluster seeds, etc.
sudo systemctl enable --now skaidb        # start now and on boot
systemctl status skaidb
journalctl -u skaidb -f                    # follow the logs
```

For a cluster, the per-host bits are easiest as env overrides in
`/etc/default/skaidb` (consistency levels are case-insensitive):

```sh
SKAIDB_BIND_ADDR=192.168.7.3
SKAIDB_SEEDS=192.168.7.3:7100,192.168.7.4:7100
SKAIDB_REPLICATION_FACTOR=2
SKAIDB_DEFAULT_READ_CONSISTENCY=ALL
SKAIDB_DEFAULT_WRITE_CONSISTENCY=ALL
```

Then `sudo systemctl restart skaidb`. Health is at
`http://<bind_addr>:7080/health` and metrics at `:9090/metrics`
(see [METRICS.md](METRICS.md)). Uninstalling leaves `/var/lib/skaidb` and the
`skaidb` account in place so data is never destroyed by a package removal.

For the **tarball** installs above there is no service unit; either run `skaidb`
under your own process manager or copy the unit from the repo's
[`packaging/skaidb.service`](../packaging/skaidb.service) and adjust paths.

## macOS

Builds are provided for both **Apple Silicon** (`aarch64-apple-darwin`) and
**Intel** (`x86_64-apple-darwin`). The binaries are not notarized, so macOS
Gatekeeper will warn the first time — clear the quarantine flag (shown below) or
right-click the binary in Finder and choose **Open**.

### Disk image (.dmg)

```sh
# Mount, copy the binaries to a directory on your PATH, unmount.
hdiutil attach skaidb-X.Y.Z-aarch64-apple-darwin.dmg
vol="/Volumes/skaidb X.Y.Z"
sudo cp "$vol/skaidb" "$vol/skaidbsh" /usr/local/bin/
hdiutil detach "$vol"

# Clear the quarantine flag so Gatekeeper allows them to run.
sudo xattr -d com.apple.quarantine /usr/local/bin/skaidb /usr/local/bin/skaidbsh
```

(Use the `x86_64-apple-darwin` dmg on Intel Macs.) There is no Homebrew tap yet.

### Tarball (macOS)

```sh
tar xzf skaidb-X.Y.Z-aarch64-apple-darwin.tar.gz
sudo install -m 0755 skaidb skaidbsh /usr/local/bin/
sudo xattr -d com.apple.quarantine /usr/local/bin/skaidb /usr/local/bin/skaidbsh
```

## Windows

The binaries are not code-signed, so SmartScreen may warn on first run
("More info" → "Run anyway").

### Zip archive

1. Download `skaidb-X.Y.Z-x86_64-pc-windows-msvc.zip` and extract it (it contains
   `skaidb.exe`, `skaidbsh.exe`, `LICENSE`, `README.md`).
2. Move the folder somewhere stable (e.g. `C:\Program Files\skaidb`) and add it
   to your `PATH`:

```powershell
# Unblock the downloaded zip, extract, and add to PATH (per-user).
Unblock-File .\skaidb-X.Y.Z-x86_64-pc-windows-msvc.zip
Expand-Archive .\skaidb-X.Y.Z-x86_64-pc-windows-msvc.zip -DestinationPath "$env:LOCALAPPDATA\skaidb"
[Environment]::SetEnvironmentVariable(
  "Path", $env:Path + ";$env:LOCALAPPDATA\skaidb", "User")
```

Open a new terminal so the `PATH` change takes effect.

### Standalone .exe

If you only want one binary, download `skaidb-X.Y.Z-x86_64-pc-windows-msvc.exe`
(server) and/or `skaidbsh-X.Y.Z-x86_64-pc-windows-msvc.exe`, rename to
`skaidb.exe` / `skaidbsh.exe`, and put them on your `PATH`.

## Build from source

Requirements: **Rust 1.93 or newer** (install via [rustup](https://rustup.rs)) and
a C toolchain is *not* required (all dependencies are pure Rust). `git` to clone.

```sh
git clone https://github.com/porcupin26/skaidb
cd skaidb

# Build optimized binaries (reproducible deps via the committed lockfile).
cargo build --release --locked

# Binaries land here:
#   target/release/skaidb
#   target/release/skaidbsh
sudo install -m 0755 target/release/skaidb target/release/skaidbsh /usr/local/bin/
```

Or install straight into Cargo's bin directory (`~/.cargo/bin`, usually already
on your `PATH`):

```sh
cargo install --path crates/skaidb-server   # installs `skaidb`
cargo install --path crates/skaidb-cli      # installs `skaidbsh`
```

Run the test suite or lints if you're hacking on it:

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

**Cross-compiling:** add the target and build with `--target`, e.g. a static
Linux binary: `rustup target add x86_64-unknown-linux-musl && cargo build
--release --target x86_64-unknown-linux-musl`. (This is exactly what the release
workflow does — see [RELEASING.md](RELEASING.md).)

## Binary-only (no package, no build)

If you just want the executable and nothing else (no system package, no system
files), grab the tarball/zip for your platform and extract **only** the binary
you need:

```sh
# Linux/macOS — extract just the server binary into the current directory.
tar xzf skaidb-X.Y.Z-x86_64-unknown-linux-gnu.tar.gz skaidb
./skaidb --version
```

```powershell
# Windows — the standalone .exe is already binary-only; just run it.
.\skaidb-X.Y.Z-x86_64-pc-windows-msvc.exe --version
```

The binaries are self-contained: they create and manage their own data directory
and have no runtime dependencies beyond the OS (the musl build has none at all).

## Run it / verify the install

```sh
skaidb --version
skaidbsh --version
```

Start the server (creates the data dir if missing):

```sh
skaidb --data-dir ./data --bind-addr 127.0.0.1 --rest-port 7080
# Every option is also a flag/env var; print the effective config and exit:
skaidb --print-config
```

Query it over REST:

```sh
curl -X POST 127.0.0.1:7080/query -d "CREATE TABLE users (PRIMARY KEY (id))"
curl -X POST 127.0.0.1:7080/query -d "INSERT INTO users (id, name) VALUES (1, 'ada')"
curl -X POST 127.0.0.1:7080/query -d '{"sql":"SELECT * FROM users"}'
curl 127.0.0.1:7080/metrics
```

Or use the shell. It connects over the network by default (picking the nearest
reachable node, with failover); `--local` opens a data directory directly with
no server:

```sh
skaidbsh --host 127.0.0.1 -e "SELECT COUNT(*) FROM users"
skaidbsh --host 127.0.0.1          # interactive REPL
skaidbsh --local ./data            # offline, against the data dir
```

To run multiple nodes, see **[CLUSTERING.md](CLUSTERING.md)** (seeds, ports,
replication factor, consistency, adding/removing nodes). For the SQL surface, see
[QUERY_SYNTAX.md](QUERY_SYNTAX.md).

## Upgrading

- **.deb / .rpm:** install the newer package the same way — it replaces the old
  one (`sudo apt install ./skaidb_NEW_amd64.deb`, `sudo dnf install ./skaidb-NEW…rpm`).
- **Tarball / zip / source:** overwrite the binaries in place. The on-disk data
  format is forward-compatible within a release line; stop the server before
  swapping the binary.

skaidb uses [SemVer](https://semver.org); see [RELEASING.md](RELEASING.md) for
how versions are cut.

## Uninstalling

```sh
# Debian/Ubuntu
sudo apt remove skaidb
# Fedora/RHEL/openSUSE
sudo dnf remove skaidb        # or: sudo rpm -e skaidb / sudo zypper remove skaidb
# Tarball / source install
sudo rm /usr/local/bin/skaidb /usr/local/bin/skaidbsh
# cargo install
cargo uninstall skaidb-server skaidb-cli
```

Your data directory (e.g. `./data`) is never touched by uninstalling — remove it
manually if you want to delete the database.
