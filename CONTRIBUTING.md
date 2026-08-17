# Contributing Guidelines

This Bitbucket repository is Atlassian's fork of `valkey-io/valkey-glide`.

For Atlassian changes, use the normal internal workflow:

1. Track work in Jira.
2. Create a branch from the Bitbucket `main` branch.
3. Keep changes focused and avoid unrelated upstream churn.
4. Run the closest relevant build or test target before opening a pull request.
5. Open a Bitbucket pull request and follow the repository's required reviewers and pipeline checks.

Fork-specific changes should stay limited to Atlassian packaging, publishing, CI, and networking behavior. Prefer upstream code for general GLIDE behavior unless an Atlassian-specific integration requires a fork change.

For upstream sync work, see [UPSTREAM_SYNC.md](./UPSTREAM_SYNC.md).

For public open-source contribution guidance, use the upstream GitHub repository instead of this fork.
