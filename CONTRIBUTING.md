# Contributing to wehoop-wnba-data

Thank you for your interest in contributing to `wehoop.wnba`! This document
covers how to file issues, propose changes, and what to expect during
review.

This project is part of the [SportsDataverse](https://sportsdataverse.org/)
family. By participating, you agree to abide by the
[Code of Conduct](CODE_OF_CONDUCT.md).

## Filing Issues

Before opening a new issue, please search existing issues to avoid
duplicates. When filing a bug:

- Use the **Bug report** template.
- Include a [minimal reprex](https://reprex.tidyverse.org/) when possible.
- Note your R version, package version, and OS.
- If reporting a parsing error against an external API, include the
  parameters (game ID, season, etc.) that triggered it so we can reproduce.

For feature requests, use the **Feature request** template and explain the
underlying problem; concrete proposals are welcome but not required.

## Proposing Changes

### Setup

```r
# Clone the repo, then from R inside the project root:
install.packages("devtools")
devtools::install_deps(dependencies = TRUE)
```

### Workflow

1. Open a GitHub issue (or comment on an existing one) to align on scope
   before sinking significant time into a PR.
2. Branch from `main` (`feat/<short-name>`, `fix/<short-name>`).
3. Make the change. Keep PRs focused — one feature or fix per PR.
4. Run the local checks below.
5. Open a PR using the
   [pull request template](.github/pull_request_template.md).

### Local Checks

For changes to R source:

```r
devtools::document()          # regenerate NAMESPACE + man/ from roxygen
devtools::test()              # run unit tests
devtools::check()             # full R CMD check (no errors, no warnings)
styler::style_pkg()           # optional but appreciated
```

For documentation:

```r
pkgdown::build_site()         # regenerate the docs site locally
```

## Code Style

- **Naming**: `snake_case` for variables, function names, and arguments;
  internal helpers prefixed with a leading dot.
- **Pipe**: prefer the native R pipe `|>` (R >= 4.1). Existing magrittr
  `%>%` is acceptable when surrounding code uses it.
- **Indentation**: 2 spaces, no tabs.
- **Strings**: double quotes.
- **Roxygen**: every exported function needs `@param` lines, an
  `@return` description, an `@examples` block (wrap network calls in
  `\dontrun{}`), and an `@export` tag. Use `@family` to group
  related functions for the pkgdown reference index.

## Commit Messages

Conventional Commits format is preferred:

```
<type>(<scope>): <short imperative summary>
```

Where `<type>` is one of `feat`, `fix`, `docs`, `test`,
`refactor`, `perf`, `chore`, `ci`, `build`. Scope is optional but
useful (e.g. `feat(nba): add play_by_play loader`).

For repos that drive scheduled scrape/compile workflows, daily umbrella
commits use the load-bearing form
`"<Sport> <Type> Updated (Start: YYYY End: YYYY)"` — preserve that
exactly when modifying the daily scripts.

## Code of Conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md).
Please review it before participating.

## Questions

For broader questions, ping us in the
[SportsDataverse Discord](https://discord.com/invite/u4cQDanRYj) or open a
GitHub Discussion if the repo has them enabled.
