# Upstream Sync

This fork tracks GitHub `valkey-io/valkey-glide` while keeping Atlassian-only packaging and Flock networking changes.

## Remotes

```bash
git remote add upstream https://github.com/valkey-io/valkey-glide.git
git fetch origin
git fetch upstream
```

`origin` is Bitbucket: `git@bitbucket.org:atlassian/valkey-glide.git`.

## Rebase Onto GitHub Main

Create the sync branch from the Bitbucket fork main:

```bash
git switch -c NOISSUE-rebase-on-oss-main-YYYYMMDD origin/main
BASE=$(git merge-base origin/main upstream/main)
git rebase --rebase-merges --onto upstream/main "$BASE"
```

Resolve conflicts by defaulting to upstream for general GLIDE code, then re-apply only fork-specific behavior.

Keep:

- Atlassian Maven coordinates and publishing config, especially `io.atlassian.valkey`.
- `bitbucket-pipelines.yml` and internal release wiring.
- Flock raw-IP/address-resolver behavior for MOVED/ASK redirects.
- JNI `AddressResolver` lifecycle fixes.

Prefer upstream:

- Generated command APIs.
- Dependency updates.
- Core timeout, async, connection, retry, and logging fixes.
- CI/test framework changes unless they directly conflict with Bitbucket publishing.

## Validate

Run at least:

```bash
cargo fmt --manifest-path java/Cargo.toml
cargo fmt --manifest-path glide-core/redis-rs/Cargo.toml
GLIDE_VERSION=dev cargo check --manifest-path java/Cargo.toml
GLIDE_VERSION=dev cargo check --manifest-path glide-core/redis-rs/Cargo.toml -p redis --features tokio-comp,cluster-async
```

Then run the closest Java build/test target available for the changed surface, for example:

```bash
cd java
PATH=/path/to/protoc-29.1/bin:$PATH ./gradlew --no-daemon :client:cleanProtobuf :client:compileJava :integTest:compileJava
```

Use `protoc` 29.1 for Java validation. Newer local `protoc` versions can generate code that requires a newer `protobuf-java` runtime than the version pinned by Gradle.

## Push

```bash
git push -u origin NOISSUE-rebase-on-oss-main-YYYYMMDD
```
