# Changelog

## Unpublished

### Added
- Nothing of note.

### Changed
- Nothing of note.

### Deprecated
- Nothing of note.

### Removed
- Nothing of note.

### Fixed
- Nothing of note.

## v0.2.0 [2022-06-28]

### Added
- Add support for algorithmic phase region.
- Add source location information to task attributes.
- Add inclusive and exclusive task duration to task attributes.

### Changed
- Many internal implementation details revised.

### Fixed
- Fixed [#15](https://github.com/adamtuft/otter/issues/15#issue-988922376) to ensure `sync_cluster_id` vertex attribute is always defined to prevent crashes when there are no barrier regions encountered.
- Fix issue causing nested parallel regions to appear detached in the output graph.
- Fix multiple issues with the application of task synchronisation constraints.
- Fix issues causing missing edges in various corner cases.

## v0.1.1 [2021-08-25]

## Changed
- Correct metadata in setup.py

## v0.1 [2021-08-25]

### Added
- Initial prototype.
