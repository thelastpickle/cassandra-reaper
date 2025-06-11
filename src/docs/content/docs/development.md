---
title: "Development"
weight: 200
---

## Cutting Releases

Cutting a release involves the following steps.

1. Consult with the community to confirm the codebase is ready for release by reviewing outstanding issues and pull requests.

2. For major releases, create a release branch from master following the naming convention: `1.0`, `1.1`, `1.2`, `1.3`, etc.

3. Generate the changelog by running:
   ```
   github-changes -o thelastpickle -r cassandra-reaper --use-commit-body -a -f changelog.tmp -b <sha-of-release> -v
   ```
   Review and edit the generated changelog to include only relevant commits, then update `CHANGELOG.md`.

4. Update the version number using:
   ```
   mvn -B versions:set "-DnewVersion=<release-version-number>"
   ```
   Version numbers should follow semantic versioning: `<major>.<minor>.<patch>` or `<major>.<minor>.<patch>-<prerelease suffix>`.

5. Create and push a tag matching the release number (omit the `v` prefix).

6. Navigate to the [GitHub tags page](https://github.com/thelastpickle/cassandra-reaper/tags) and publish a release from the new tag.

7. Update the GitHub release description with the latest version's changelog entries.

8. Monitor the [GitHub Actions](https://github.com/thelastpickle/cassandra-reaper/actions) workflow and verify CI completion for the release.

9. Forward merge the release changelog commit to `master` branch as detailed in the following section to maintain branches synchronization.

## Forward Merging

Reaper practices forward merging commits.

Fixes and improvements required to release branches are first committed to those branches. These changes are merged forward onto master afterwards.

An example where a bugfix developed and approved for the release branch 1.4 is to be merged;
```

# switch to master
git checkout master
# create the empty merge commit
git merge 1.4 -s ours
# cherry-pick the change using the sha noted above, and make any manual adjustments required against the master branch
git cherry-pick -n <sha>
# commit amend the forward ported changes into the merge commit
git commit -a --amend

# push both 1.4 and master branches at the same time
git push origin 1.4 master --atomic
```

For more information on the value of forward merging, and the principles of "merge down, copy up" and the "tofu scale: firm above, soft below", see

 - https://www.perforce.com/perforce/conferences/us/2005/presentations/Wingerd.pdf
 - https://www.youtube.com/watch?v=AJ-CpGsCpM0

## Releasing Docs

The [cassandra-reaper.io](http://cassandra-reaper.io/) pages are not updated automatically when doing a release. Deploying new docs is a manual process for now. To do the deploy:

- Ensure you're on `master` branch that's up to date with the upstream repo.
- Navigate to the `src/docs`.
- Run `make build`.
- Commit changes in the `docs` folder (relative to the root of the repository).
- Open a PR with the changes.

## Editor Config

Reaper uses [Spotless](https://github.com/diffplug/spotless/tree/main/plugin-maven) to enforce consistent code style across the repository. The Google Code Style guidelines are automatically applied to all files when running `mvn spotless:apply`.

Code style compliance is verified during the Maven build process. You can also manually check for style violations by running `mvn spotless:check`.