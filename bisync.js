#!/usr/bin/env node

const argv = require('yargs')
  .usage('Usage: $0 --db [directory] --filter-from [file] root1 root2')
  .string('db')
  .nargs('db', 1)
  .describe('db', 'Location of bisync state database.  Must be unique to each sync pair.')
  .string('filter-from')
  .nargs('filter-from', 1)
  .describe('filter-from', 'Read filtering patterns from a file.')
  .boolean('fast-list')
  .nargs('fast-list', 0)
  .describe('fast-list', 'Use recursive list if available. Uses more memory but fewer transactions.')
  .boolean('drive-skip-gdocs')
  .nargs('drive-skip-gdocs', 0)
  .describe('drive-skip-gdocs', 'Skip google documents in all listings. If given, gdocs practically become invisible to rclone.')
  .boolean('empty-dir-placeholder-root1')
  .nargs('empty-dir-placeholder-root1', 0)
  .describe('empty-dir-placeholder-root1', 'Maintain empty directory placeholder file .rclone_keep on root 1.')
  .boolean('empty-dir-placeholder-root2')
  .nargs('empty-dir-placeholder-root2', 0)
  .describe('empty-dir-placeholder-root2', 'Maintain empty directory placeholder file .rclone_keep on root 2.')
  .boolean('keep-root1')
  .nargs('keep-root1', 0)
  .describe('keep-root1', 'Resolve conflicts by keeping changes made to root 1.  Default mode.')
  .boolean('keep-root2')
  .nargs('keep-root2', 0)
  .describe('keep-root2', 'Resolve conflicts by keeping changes made to root 2.')
  .default({'keep-root1': undefined, 'keep-root2': undefined})
  .conflicts('keep-root1', 'keep-root2')
  .demandOption(['db'])
  .demandCommand(2, 2, "Please specify two rclone paths to sync.", "Please specify two rclone paths to sync.")
  .argv;

const untildify = require('untildify');
const path = require('path');
const encode = require('encoding-down');
const leveldown = require('leveldown');
const levelup = require('levelup');
const lockfile = require('proper-lockfile');
const fs = require('fs-extra');

const {level_get, join} = require('./utils');
const {rclone_lsjson, load_previous_objects, empty_dir_placeholder_maintain} = require('./rclone-ops');
const {generate_changes,
       relate_objects,
       analyze_changes,
       persist_duplicate_pairs} = require('./bisync-ops');

async function main() {
  const STATE_PATH = await validateStatePath(argv.db);

  const release = await lockfile.lock(`${STATE_PATH}LOCK`);
  try {
    await setupSync(STATE_PATH);
  } catch(err) {
    console.error(err);
  }
  await release();
}

async function validateStatePath(db_path) {
  let normalized_db_path = path.normalize(untildify(db_path));

  let exists = null;
  try {
    let stinfo = await fs.stat(normalized_db_path);
    if(stinfo.isFile()) {
      throw new Error(`Bisync state database ${db_path} must be a directory`);
    }
    exists = true;
  } catch(err) {
    if(err.code === 'ENOENT') {
      exists = false;
    } else {
      throw err;
    }
  }

  if(!exists) {
    await fs.mkdir(normalized_db_path);
  }

  if(!normalized_db_path.endsWith(path.sep)) {
    normalized_db_path = `${normalized_db_path}${path.sep}`;
  }

  const lock_path = `${normalized_db_path}LOCK`;
  let lock_exists = null;
  try {
    let stinfo = await fs.stat(lock_path);
    lock_exists = true;
  } catch(err) {
    if(err.code === 'ENOENT') {
      exists = false;
    } else {
      throw err;
    }
  }

  if(!lock_exists) {
    const f = await fs.open(lock_path, "w");
    await fs.close(f);
  }

  return normalized_db_path;
}

async function setupSync(STATE_PATH) {
  const metadb = levelup(encode(leveldown(`${STATE_PATH}meta`), { valueEncoding: 'json' }));
  const root1db = levelup(encode(leveldown(`${STATE_PATH}root1`), { valueEncoding: 'json' }));
  const root2db = levelup(encode(leveldown(`${STATE_PATH}root2`), { valueEncoding: 'json' }));

  const app = {argv, STATE_PATH, metadb, root1db, root2db};
  try {
    await validateSyncProcess(app);
  } catch(err) {
    console.error(err);
  }

  await metadb.close();
  await root1db.close();
  await root2db.close();
}

async function validateSyncProcess(app) {
  if(app.argv['keep-root1'] === undefined && app.argv['keep-root2'] === undefined) {
    app.argv['keep-root1'] = true;
  }

  if(app.argv._[0].endsWith("/") || app.argv._[0].endsWith("\\")) {
    app.argv._[0] = app.argv._[0].slice(0, -1)
  }

  if(app.argv._[1].endsWith("/") || app.argv._[1].endsWith("\\")) {
    app.argv._[1] = app.argv._[1].slice(0, -1)
  }

  let root1uri = await level_get(app.metadb, "root1uri");
  if(root1uri === undefined) {
    app.metadb.put("root1uri", app.argv._[0]);
    root1uri = argv._[0];
  }

  let root2uri = await level_get(app.metadb, "root2uri");
  if(root2uri === undefined) {
    app.metadb.put("root2uri", app.argv._[1]);
    root2uri = argv._[1];
  }

  if(root1uri !== app.argv._[0] || root2uri !== app.argv._[1]) {
    throw new Error("Sync roots have changed.  Use a different state db.")
  }

  app.root1uri = root1uri;
  app.root2uri = root2uri;

  if(app.argv['filter-from'] !== undefined) {
    app.argv['filter-from'] = path.normalize(untildify(app.argv['filter-from']));
  }

  await beginSync(app);
}

async function beginSync(app) {
  let r1_changes, r2_changes, changesA, changesB, duplicate_pairs, conflicts;

  if(app.argv['empty-dir-placeholder-root1']) {
    await empty_dir_placeholder_maintain(app, app.root1uri);
  }

  if(app.argv['empty-dir-placeholder-root2']) {
    await empty_dir_placeholder_maintain(app, app.root2uri);
  }

  const root1_previous_objects = await load_previous_objects(app.root1db);
  const root1_current_objects = await rclone_lsjson(app, app.root1uri);
  r1_changes = generate_changes(app, app.root1db, app.root1uri, app.root2db, app.root2uri, root1_previous_objects, root1_current_objects);
  console.log(`All root 1 changes: ${r1_changes.length}`);

  const root2_previous_objects = await load_previous_objects(app.root2db);
  const root2_current_objects = await rclone_lsjson(app, app.root2uri);
  r2_changes = generate_changes(app, app.root2db, app.root2uri, app.root1db, app.root1uri, root2_previous_objects, root2_current_objects);
  console.log(`All root 2 changes: ${r2_changes.length}`);

  relate_objects(root1_current_objects, root2_current_objects, root1_previous_objects, root2_previous_objects);

  [changesA, changesB, duplicate_pairs, conflicts] = analyze_changes(app, r1_changes, r2_changes, root1_current_objects, root2_current_objects);

  console.log(`Duplicate changes ignored: ${duplicate_pairs.length}`);
  console.log(`Conflicting changes detected: ${conflicts.length}`);

  await persist_duplicate_pairs(duplicate_pairs);

  for(const change of changesA.reverse()) {
    await change.syncTo();
  }

  for(const change of changesB.reverse()) {
    await change.syncTo();
  }

  for(const conflict of conflicts.reverse()) {
    console.log(conflict.toString());
    await conflict.resolve();
  }
}

// Map SIGINT & SIGTERM to process exit so that proper-lockfile removes the lockfile automatically
process
  .once('SIGINT', () => process.exit(1))
  .once('SIGTERM', () => process.exit(1));

main().catch(console.error);
