set working-directory := './'

_default:
    @just --list

# Tag a package release and push the tag to origin.
#
# This repository is a single root Go module, so releases use plain semver tags
# such as "v1.2.3". No goreleaser, no changelog generation - just a correct,
# minimal, annotated tag pushed to origin.
#
# Usage:
#   just release v1.2.3
# just release 1.2.3      # "v" prefix is added if missing
[group('release')]
release version:
    #!/usr/bin/env bash
    set -euo pipefail

    if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        echo "error: not a git repository (run 'git init' first)" >&2
        exit 1
    fi

    if [ ! -f "go.mod" ]; then
        echo "error: go.mod not found - is this a Go module repo?" >&2
        exit 1
    fi

    version="{{ version }}"
    [[ "$version" == v* ]] || version="v${version}"

    if ! [[ "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$ ]]; then
        echo "error: '$version' is not a valid semver (expected vX.Y.Z)" >&2
        exit 1
    fi

    if [[ -n "$(git status --porcelain)" ]]; then
        echo "error: working tree is dirty - commit or stash before tagging" >&2
        exit 1
    fi

    tag="$version"

    if git rev-parse "$tag" >/dev/null 2>&1; then
        echo "error: tag '$tag' already exists" >&2
        exit 1
    fi

    echo "==> Tagging ${tag} at $(git rev-parse --short HEAD)"
    git tag -a "$tag" -m "$tag"

    echo "==> Pushing ${tag} to origin"
    git push origin "$tag"

    echo "released ${tag}"
