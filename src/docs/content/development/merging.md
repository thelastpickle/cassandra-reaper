+++
title = "Forward Merging"
menuTitle = "Forward Merging"
weight = 25
+++

Reaper practices forward merging commits.

Fixes and improvements required to release branches are first committed to those branches. These changes are merged forward onto master afterwards.

An example where a bugfix developed and approved for the release branch 1.4 is to be merged;

### 1. Rebase the work one last time off the release branch
```
git checkout bob/1.4_bugfix
git rebase 1.4
git push
```

### 2. Switch to the Release branch and fast-forward merge in the fix
```
git checkout 1.4
git merge bob/1.4_bugfix # take note of the sha of the fast-forward merge commit that just happened
```

### 3. Prepare to merge the commits into the `master` branch
```
git checkout master
git merge <RELEASE_BRANCH> -s ours
```

### 4. Cherry-pick the change using the sha noted above

Make any manual adjustments required against the master branch
```
git cherry-pick -n <sha>
git commit -a --amend
```

### 5. push both the release branch and `master` branches at the same time
```
git push origin <REALSE BRANCH> master --atomic
```

For more information on the value of forward merging, and the principles of "merge down, copy up" and the "tofu scale: firm above, soft below", see

 - https://www.perforce.com/perforce/conferences/us/2005/presentations/Wingerd.pdf
 - https://www.youtube.com/watch?v=AJ-CpGsCpM0