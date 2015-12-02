# Change Log


## [Unreleased] - unreleased


## [2] - 2015-12-02
### Fixed
- Better fine grain execution of tasks that may emit in bulk.

### Added
- Buffering mode.
- Gathering mode.
- `IOCell.append_tier` which adds a tier who sources the last added tier like
  a pipe.

### Removed
- `IOCell.tier_coroutine` is now implicitly handled by `IOCell.tier`.


## [1] - 2015-11-15
### Added
- First release


[unreleased]: https://github.com/mayfield/ecmcli/compare/v2...HEAD
[2]: https://github.com/mayfield/ecmcli/compare/v1...v2
[1]: https://github.com/mayfield/ecmcli/compare/bdf2e3f359a3761982fb19edf31c8a1e57209eec...v1
