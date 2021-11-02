devtools::install_github('ohdsi/OhdsiRTools')

OhdsiRTools::createRenvLockFile(rootPackage = "SkeletonCohortCharacterization",
                                includeRootPackage = FALSE,
                                additionalRequiredPackages = c("DT", 
                                                               "shiny", 
                                                               "shinydashboard")
                                )
