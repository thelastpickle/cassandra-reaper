+++
[menu.docs]
name = "Development"
weight = 200
+++

# Forward Merging

Reaper practices forward merging commits.

Fixes and improvements required to release branches are first committed to those branches. These changes are merged forward onto master afterwards.

An example where a bugfix developed and approved for the release branch 1.4 is to be merged;
```
# first rebase the work one last time off the latest 1.4 branch
git checkout bob/1.4_bugfix
git rebase 1.4
git push

# switch to the 1.4 branch and fast-forward merge in the fix
git checkout 1.4
git merge bob/1.4_bugfix # take note of the sha of the fast-forward merge commit that just happened

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


# Cutting Releases

Cutting a release involves the following steps.

- Check with the community if the codebase is ready for the release, this includes checking any outstanding issues or pull requests in progress.
- For a major release, create the release branch off master. Release branches follow the naming: `1.0`, `1.1`, `1.2`, `1.3`, etc.
- Check that the release branch has green build status on both Travis and CircleCI.
- Update `CHANGELOG.md` using [github-change](https://github.com/lalitkapoor/github-changes) and the following command (editing and removing non-pertinent commits): `github-changes -o thelastpickle -r cassandra-reaper --use-commit-body -k 5aef4bf7c2d9d0799f3c86e7aa1db512073b50d8 -f changelog.tmp -b <sha-of-release> -v`
- Create the release tag using the maven-release-plugin and the following command: `mvn release:prepare -Dtag=<release-version-number>`
- Do not run `mvn release:perform`, as Travis will do the build and deployment of the released version tag.
- Monitor the Travis build for the released version tag, ensuring the build succeeds through to the deployment task. This is the last task in the job.
- On the GitHub release page, edit the description to include the changelog from above for the latest version.
- Forward port (without carrying the changes) the release to master. Use the following commands: `git checkout master; git merge <release-branch> -s ours; git push`

