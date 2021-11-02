Instructions To Share Package
===================

If you want to make the package available for others to run, we suggest:

- Replace the current readme.md with the readmeSharing.md.  The readmeSharing.md contains instructions for installing the package from the 'ohdsi-studies' github repo.
- update the new readme.md with the study information
- Potentially update the renv lockfile (only run this if you modified the package and it requires new or updated libaries):
```{r setup, include=FALSE}
# Create the Renv lock file with all the R libraries and versions
# make sure you are in the study location (check with getwd())
OhdsiRTools::createRenvLockFile("SkeletonCohortCharacterization")

```

- Share the package by adding it to the 'ohdsi-studies' github repo.  You may need to ask for the 'ohdsi-studies' github to be created.