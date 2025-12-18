# Task 4 – Optimization Component (Restic)

The optimization was implemented in Restic by introducing lazy initialization
of the master index to reduce metadata memory footprint.

Repository and branch:
https://github.com/sanjadomjanic-cmyk/restic/tree/optimized-index-caching

Key file:
internal/repository/repository.go
