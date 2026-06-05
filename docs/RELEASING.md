# Releasing & versioning

Releases are fully automated by GitHub Actions. **Every push to `main` cuts a
new versioned GitHub Release** with binaries and packages for every supported
platform — there is nothing to run by hand.

## What happens on a push to `main`

[`.github/workflows/release.yml`](../.github/workflows/release.yml) runs four
stages in order; a failure in any stage stops the release:

1. **gate** — `cargo clippy -D warnings` + `cargo test` on the whole workspace.
   Broken code never ships.
2. **version** — compute the next [SemVer](https://semver.org) from the commits
   since the last tag (see below), write it into the workspace `Cargo.toml`,
   commit it as `chore(release): vX.Y.Z [skip ci]`, and create + push the
   `vX.Y.Z` tag.
3. **build** — a cross-compile matrix builds the `skaidb` server and `skaidb-cli`
   for every target and packages them (see the matrix below).
4. **release** — collect every artifact, generate `SHA256SUMS`, and publish the
   GitHub Release for the tag (with auto-generated notes).

The bump commit is tagged `[skip ci]`, so publishing a release does **not**
trigger another release. To push to `main` *without* cutting a release (e.g. a
docs typo), put `[skip ci]` in your commit message.

## Versioning (SemVer from Conventional Commits)

The bump level is derived from the commit subjects/bodies since the previous tag,
following [Conventional Commits](https://www.conventionalcommits.org):

| Commit contains                                  | Bump  |
|--------------------------------------------------|-------|
| `<type>!:` or a `BREAKING CHANGE` footer         | major |
| `feat:` / `feat(scope):`                         | minor |
| anything else (`fix:`, `chore:`, `docs:`, …)     | patch |

The highest level among the new commits wins. The **first** release (no prior
tag) ships the version already in `Cargo.toml` as-is. All crates inherit the
single `[workspace.package] version`, so one bump versions the whole workspace.

Examples: `feat: add UNION` → minor; `fix: off-by-one` → patch;
`refactor!: drop legacy API` (or a `BREAKING CHANGE:` footer) → major.

## Artifacts per release

| Platform | Target | Files |
|----------|--------|-------|
| Linux x86_64 (glibc) | `x86_64-unknown-linux-gnu` | `.tar.gz`, `.deb`, `.rpm` |
| Linux aarch64 (glibc) | `aarch64-unknown-linux-gnu` | `.tar.gz`, `.deb`, `.rpm` |
| Linux x86_64 (static) | `x86_64-unknown-linux-musl` | `.tar.gz` |
| macOS Intel | `x86_64-apple-darwin` | `.tar.gz`, `.dmg` |
| macOS Apple Silicon | `aarch64-apple-darwin` | `.tar.gz`, `.dmg` |
| Windows x86_64 | `x86_64-pc-windows-msvc` | `.zip`, raw `.exe` |

Plus a `SHA256SUMS` covering every file. Each archive/package bundles both the
`skaidb` server and the `skaidb-cli` shell, the `LICENSE`, and the `README`. The
`.deb`/`.rpm` install the binaries to `/usr/bin`. Packaging uses
[`nfpm`](https://nfpm.goreleaser.com) (deb/rpm), `hdiutil` (dmg), and native
`tar`/`Compress-Archive` for the rest.

## Requirements / notes

- The workflow pushes the bump commit and tag with the built-in `GITHUB_TOKEN`
  (`permissions: contents: write`). If `main` is a protected branch, allow the
  Actions bot to push, or relax the protection — otherwise the version stage
  cannot commit the bump.
- Pull requests and non-`main` pushes run the lighter
  [`ci.yml`](../.github/workflows/ci.yml) gate (clippy + test) only.
- `workflow_dispatch` lets you trigger a release manually from the Actions tab.
