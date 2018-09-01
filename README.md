# RClone Bisync

RClone Bi-Directional sync implementation written in Node.js sitting on-top of the rclone command line interface.

## Current status

Consider this codebase experimental.

## Usage

```
$ node bisync.js
Usage: bisync.js --db [directory] --filter-from [file] root1 root2

Options:
  --help                         Show help                             [boolean]
  --version                      Show version number                   [boolean]
  --db                           Location of bisync state database.  Must be
                                 unique to each sync pair.   [string] [required]
  --filter-from                  Read filtering patterns from a file.   [string]
  --fast-list                    Use recursive list if available. Uses more
                                 memory but fewer transactions.        [boolean]
  --drive-skip-gdocs             Skip google documents in all listings. If
                                 given, gdocs practically become invisible to
                                 rclone.                               [boolean]
  --empty-dir-placeholder-root1  Maintain empty directory placeholder file
                                 .rclone_keep on root 1.               [boolean]
  --empty-dir-placeholder-root2  Maintain empty directory placeholder file
                                 .rclone_keep on root 2.               [boolean]
  --keep-root1                   Resolve conflicts by keeping changes made to
                                 root 1.  Default mode.                [boolean]
  --keep-root2                   Resolve conflicts by keeping changes made to
                                 root 2.                               [boolean]

```
