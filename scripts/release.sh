#!/usr/bin/env bash

BRANCH_NAME="$(git branch --show-current)"

if [ "$BRANCH_NAME" != "main" ]; then
  printf "current branch is not main"
  exit
fi

VERSION="$1"
printf "releasing %s" "$VERSION"
git tag "$VERSION"
git push origin main --tags
