const path = require('path');

const {rclone_lsjson, rclone_copy,
       rclone_rmdir, rclone_deletefile,
       rclone_mkdir, rclone_load_object} = require('./rclone-ops');
const {RFile, RDirectory, RDeletedFile, RDeletedDirectory} = require('./rclone-fs');
const {DuplicateConflictSet, RenameB, SkipB, SkipA} = require('./bisync-conflict.js');
const {dirname, join, PROCEED, ALREADY_APPLIED, HALT} = require('./utils');

class Change {
  constructor (app, db, uri, remote_db, remote_uri, current, previous) {
    this.app = app;
    this.db = db;
    this.uri = uri;
    this.remote_db = remote_db;
    this.remote_uri = remote_uri;
    this.current = current;
    this.previous = previous;
  }

  async validateSync(opts) {
    if(!opts) opts = {};

    const updated_current = await this.loadCurrent();
    const updated_related = await this.loadRelated();

    if( ( (updated_current === undefined && updated_related === undefined) ||
          (updated_current && updated_current.equals(updated_related)) ) &&
        this.current.equals(updated_current) ) {

      return ALREADY_APPLIED;
    }

    if(!(this.current.equals(updated_current))) {
      console.log(`Not syncing : ${this.type} of ${this.current.path} target changed state while executing on ${this.uri}`);
      return HALT;
    }

    if(!opts.local_test_only) {
      // undefined values here represent objects that don't exist - perhaps deleted
      if( (this.target.related === undefined && updated_related !== undefined) ||
           (this.target.related && !(this.target.related.equals(updated_related))) ) {
        console.log(`Not syncing : ${this.type} of ${this.target.path} target changed state while executing on ${this.remote_uri}`);
        return HALT;
      }
    }

    return PROCEED;
  }

  async loadCurrent() {
    return await rclone_load_object(this.app, this.uri, this.current.path);
  }

  async loadRelated() {
    return await rclone_load_object(this.app, this.remote_uri, this.target.path);
  }

  async syncTo(opts) {
    const status = await this.validateSync(opts);
    if(status === PROCEED) {
      console.log(`Syncing : ${this.type} of ${this.target.path} from ${this.uri} to ${this.remote_uri}`);

      await this.apply();

      await this.persist();
      await this.persistRemote();
    } else if(status === ALREADY_APPLIED) {
      console.log(`Not syncing : ${this.type} of ${this.current.path} change already applied on ${this.remote_uri}`);

      await this.persist();
      await this.persistRemote();
    }
  }

  toString() {
    return `${this.constructor.name} Change - ${this.type} ${join(this.uri, this.target.path)}`;
  }
}

class Modify extends Change {
  get type() {
    return "modify";
  }

  async apply() {
    if(this.target instanceof RFile) {
      const parent_path = dirname(this.target.path);
      await rclone_copy(this.app, join(this.uri, this.target.path), join(this.remote_uri, parent_path));
    }
  }

  async persist() {
    await this.db.put(this.target.path, this.target.toJSON());
  }

  async persistRemote() {
    const updated_related = await this.loadRelated();

    if(updated_related) {
      await this.remote_db.put(this.target.path, updated_related.toJSON());
    } else {
      throw new Error("Error while persisting change to db.  Modify detected but no remote file found.");
    }
  }

  get target() {
    return this.current;
  }

  equals(remote_change) {
    return (remote_change instanceof Modify && this.target.equals(remote_change.target));
  }
}

class Add extends Change {
  get type() {
    return "add";
  }

  async apply() {
    if(this.target instanceof RDirectory) {
      await rclone_mkdir(this.app, join(this.remote_uri, this.target.path));
    } else {
      const parent_path = dirname(this.target.path);
      await rclone_copy(this.app, join(this.uri, this.target.path), join(this.remote_uri, parent_path));
    }
  }

  async persist() {
    await this.db.put(this.target.path, this.target.toJSON());
  }

  async persistRemote() {
    const updated_related = await this.loadRelated();

    if(updated_related) {
      await this.remote_db.put(this.target.path, updated_related.toJSON());
    } else {
      throw new Error("Error while persisting change to db.  Add detected but no remote file found.");
    }
  }

  get target() {
    return this.current;
  }

  equals(remote_change) {
    return (remote_change instanceof Add && this.target.equals(remote_change.target));
  }
}

class Remove extends Change {
  get type() {
    return "remove";
  }

  async apply() {
    if(this.target instanceof RDirectory) {
      await rclone_rmdir(this.app, join(this.remote_uri, this.target.path));
    } else {
      await rclone_deletefile(this.app, join(this.remote_uri, this.target.path));
    }
  }

  async persist() {
    await this.db.del(this.target.path);
  }

  async persistRemote() {
    const updated_related = await this.loadRelated(this.remote_uri);

    if(updated_related === undefined) {
      await this.remote_db.del(this.target.path);
    } else {
      throw new Error("Error while persisting change to db.  Detected a remote file.");
    }
  }

  get target() {
    return this.previous;
  }

  equals(remote_change) {
    return (remote_change instanceof Remove && this.target.equals(remote_change.target));
  }
}

class OutOfSyncError extends Error {
  constructor (changeA, changeB) {
    super(`Remotes out of sync - ${changeA.type} of ${changeA.target.type} ${join(changeA.uri, changeA.target.path)} with ${changeB.type} ` +
          `of ${changeB.target.type} ${join(changeB.uri, changeB.target.path)}.`);
  }
}

function generate_changes(app, db, uri, remote_db, remote_uri, previous, current) {
  const changes = [];

  for(const obj of current.objs) {
    if( obj.path in previous.paths ) {
      const prev_obj = previous.paths[obj.path];
      if(!obj.equals(prev_obj)) {
        const change = new Modify(app, db, uri, remote_db, remote_uri, obj, prev_obj);
        changes.push(change);
      }
    } else {
      const change = new Add(app, db, uri, remote_db, remote_uri, obj);
      changes.push(change);
    }
  }

  for(const obj of previous.objs) {
    if( !(obj.path in current.paths) ) {
      let current_obj;
      const name = path.basename(obj.path);
      if(obj instanceof RDirectory) {
        current_obj = new RDeletedDirectory({Path: obj.path, Name: name, Size: -1, MimeType: obj.mimetype, IsDir: true})
      } else {
        current_obj = new RDeletedFile({Path: obj.path, Name: name, Size: -1, MimeType: obj.mimetype, IsDir: false})
      }
      const change = new Remove(app, db, uri, remote_db, remote_uri, current_obj, obj);
      changes.push(change);
    }
  }

  return changes;
}

function relate_objects(root1_current_objects, root2_current_objects, root1_previous_objects, root2_previous_objects) {
  for(const root1_current_object of root1_current_objects.objs) {
    if( root1_current_object.path in root2_current_objects.paths ) {
      const root2_current_object = root2_current_objects.paths[root1_current_object.path];
      root1_current_object.related = root2_current_object;
      root2_current_object.related = root1_current_object;
    }
  }

  for(const root1_previous_object of root1_previous_objects.objs) {
    if( root1_previous_object.path in root2_current_objects.paths ) {
      const root2_current_object = root2_current_objects.paths[root1_previous_object.path];
      root1_previous_object.related = root2_current_object;
    }
  }

  for(const root2_previous_object of root2_previous_objects.objs) {
    if( root2_previous_object.path in root1_current_objects.paths ) {
      const root1_current_object = root1_current_objects.paths[root2_previous_object.path];
      root2_previous_object.related = root1_current_object;
    }
  }
}

async function persist_duplicate_pairs(duplicate_pairs) {
  for(const pair of duplicate_pairs) {
    if(pair[0]) {
      await pair[0].persist();
    }
    if(pair[1]) {
      await pair[1].persist();
    }
  }
}

function conflictsWith(app, changeA, changeB, changesA_current_objects, changesB_current_objects) {
  let conflict = undefined;

  // Note - RDirectory's will never generate a modify change

  if(changeA.target instanceof RFile && changeB.target instanceof RDirectory) {
    if(changeA instanceof Remove && changeB instanceof Add) {
      if(changeA.target.path === changeB.target.path) {
        throw new OutOfSyncError(changeA, changeB);
      } else if(changeB.target.parentOf(changeA.target)) {
        // shouldn't happen - but not a conflict
        // no conflict - both changes can safely by applied - adding a directory that already exists doesn't error
      } else if(changeA.target.parentOf(changeB.target)) {
        throw new OutOfSyncError(changeA, changeB);
      }
    } else if(changeA instanceof Remove && changeB instanceof Remove) {
      if(changeA.target.path === changeB.target.path) {
        throw new OutOfSyncError(changeA, changeB);
      } else if(changeB.target.parentOf(changeA.target)) {
        // no direct conflict - both changes can safely by applied
      } else if(changeA.target.parentOf(changeB.target)) {
        throw new OutOfSyncError(changeA, changeB);
      }
    } else if( (changeA instanceof Add || changeA instanceof Modify) && changeB instanceof Add ) {
      if(changeA.target.path === changeB.target.path) {
        conflict = new RenameB(app, changeA, changeB, changesA_current_objects, changesB_current_objects);
      } else if(changeB.target.parentOf(changeA.target)) {
        // no direct conflict - both changes can safely by applied
      } else if(changeA.target.parentOf(changeB.target)) {
        conflict = new RenameB(app, changeA, changeB, changesA_current_objects, changesB_current_objects);
      }
    } else if( (changeA instanceof Add || changeA instanceof Modify) && changeB instanceof Remove ) {
      if(changeA.target.path === changeB.target.path) {
        throw new OutOfSyncError(changeA, changeB);
      } else if(changeB.target.parentOf(changeA.target)) {
        conflict = new SkipB(app, changeA, changeB, changesA_current_objects, changesB_current_objects);
      } else if(changeA.target.parentOf(changeB.target)) {
        throw new OutOfSyncError(changeA, changeB);
      }
    }
  } else if(changeA.target instanceof RDirectory && changeB.target instanceof RFile) {
    if( changeA instanceof Remove && (changeB instanceof Add || changeB instanceof Modify) ) {
      if(changeA.target.path === changeB.target.path) {
        throw new OutOfSyncError(changeA, changeB);
      } else if(changeB.target.parentOf(changeA.target)) {
        throw new OutOfSyncError(changeA, changeB);
      } else if(changeA.target.parentOf(changeB.target)) {
        conflict = new SkipA(app, changeA, changeB, changesA_current_objects, changesB_current_objects);
      }
    } else if(changeA instanceof Remove && changeB instanceof Remove) {
      if(changeA.target.path === changeB.target.path) {
        throw new OutOfSyncError(changeA, changeB);
      } else if(changeB.target.parentOf(changeA.target)) {
        throw new OutOfSyncError(changeA, changeB);
      } else if(changeA.target.parentOf(changeB.target)) {
        // no direct conflict - both changes can safely by applied
      }
    } else if( changeA instanceof Add && (changeB instanceof Add || changeB instanceof Modify) ) {
      if(changeA.target.path === changeB.target.path) {
        conflict = new RenameB(app, changeA, changeB, changesA_current_objects, changesB_current_objects);
      } else if(changeB.target.parentOf(changeA.target)) {
        conflict = new RenameB(app, changeA, changeB, changesA_current_objects, changesB_current_objects);
      } else if(changeA.target.parentOf(changeB.target)) {
        // no direct conflict - both changes can safely by applied
      }
    } else if( changeA instanceof Add && changeB instanceof Remove ) {
      if(changeA.target.path === changeB.target.path) {
        throw new OutOfSyncError(changeA, changeB);
      } else if(changeB.target.parentOf(changeA.target)) {
        throw new OutOfSyncError(changeA, changeB);
      } else if(changeA.target.parentOf(changeB.target)) {
        // shouldn't happen - but not a conflict
        // no conflict - both changes can safely by applied - adding a directory that already exists doesn't error
      }
    }
  } else if(changeA.target instanceof RFile && changeB.target instanceof RFile) {
    if( changeA instanceof Remove && (changeB instanceof Add || changeB instanceof Modify) ) {
      if(changeA.target.path === changeB.target.path) {
        conflict = new SkipA(app, changeA, changeB, changesA_current_objects, changesB_current_objects);
      } else if(changeB.target.parentOf(changeA.target)) {
        throw new OutOfSyncError(changeA, changeB);
      } else if(changeA.target.parentOf(changeB.target)) {
        throw new OutOfSyncError(changeA, changeB);
      }
    } else if( (changeA instanceof Add || changeA instanceof Modify) && (changeB instanceof Add || changeB instanceof Modify) ) {
      if(changeA.target.path === changeB.target.path) {
        conflict = new RenameB(app, changeA, changeB, changesA_current_objects, changesB_current_objects);
      } else if(changeB.target.parentOf(changeA.target)) {
        conflict = new RenameB(app, changeA, changeB, changesA_current_objects, changesB_current_objects);
      } else if(changeA.target.parentOf(changeB.target)) {
        conflict = new RenameB(app, changeA, changeB, changesA_current_objects, changesB_current_objects);
      }
    } else if( (changeA instanceof Add || changeA instanceof Modify) && changeB instanceof Remove ) {
      if(changeA.target.path === changeB.target.path) {
        conflict = new SkipB(app, changeA, changeB, changesA_current_objects, changesB_current_objects);
      } else if(changeB.target.parentOf(changeA.target)) {
        throw new OutOfSyncError(changeA, changeB);
      } else if(changeA.target.parentOf(changeB.target)) {
        throw new OutOfSyncError(changeA, changeB);
      }
    }
  } else if(changeA.target instanceof RDirectory && changeB.target instanceof RDirectory) {
    if( changeA instanceof Remove && changeB instanceof Add ) {
      if(changeA.target.path === changeB.target.path) {
        throw new OutOfSyncError(changeA, changeB);
      } else if(changeB.target.parentOf(changeA.target)) {
        throw new OutOfSyncError(changeA, changeB);
      } else if(changeA.target.parentOf(changeB.target)) {
        conflict = new SkipA(app, changeA, changeB, changesA_current_objects, changesB_current_objects);
      }
    } else if( changeA instanceof Add && changeB instanceof Remove ) {
      if(changeA.target.path === changeB.target.path) {
        throw new OutOfSyncError(changeA, changeB);
      } else if(changeB.target.parentOf(changeA.target)) {
        conflict = new SkipB(app, changeA, changeB, changesA_current_objects, changesB_current_objects);
      } else if(changeA.target.parentOf(changeB.target)) {
        throw new OutOfSyncError(changeA, changeB);
      }
    }
  }

  return conflict;
}

function analyze_changes(app, r1_changes, r2_changes, root1_current_objects, root2_current_objects) {
  const duplicate_pairs = [];
  let conflicts = [];
  let changesA, changesA_current_objects;
  let changesB, changesB_current_objects;

  if(app.argv['keep-root1'] === true) {
    changesA = r1_changes;
    changesA_current_objects = root1_current_objects;
    changesB = r2_changes;
    changesB_current_objects = root2_current_objects;
  } else {
    changesA = r2_changes;
    changesA_current_objects = root2_current_objects;
    changesB = r1_changes;
    changesB_current_objects = root1_current_objects;
  }

  // 1st pass
  let pruneA = new Set();
  let pruneB = new Set();

  for(const [idxA, changeA] of changesA.entries()) {
    for(const [idxB, changeB] of changesB.entries()) {
      if(changeA.equals(changeB)) {
        duplicate_pairs.push([changeA, changeB]);
        pruneA.add(idxA);
        pruneB.add(idxB);
        continue;
      }

      const conflict = conflictsWith(app, changeA, changeB, changesA_current_objects, changesB_current_objects);
      if(conflict !== undefined) {
        conflicts.push(conflict);
        pruneA.add(idxA);
        pruneB.add(idxB);
        continue;
      }
    }
  }

  changesA = changesA.filter( (change,idx) => !pruneA.has(idx) );
  changesB = changesB.filter( (change,idx) => !pruneB.has(idx) );

  // 2nd pass
  // conflict resolution may leave behind untracked changes on a previous run - verify remaining objects don't already match
  pruneA = new Set();
  pruneB = new Set();

  for(const [idxA, changeA] of changesA.entries()) {
    for(const [idxB, changeB] of changesB.entries()) {
      const changeB_current = changesB_current_objects.paths[changeA.target.path]
      if( (changeA.current === undefined && changeB_current === undefined) || (changeA.current && changeA.current.equals(changeB_current)) ) {
        if(!pruneA.has(idxA)) {
          console.log(`${changeA} already in sync on ${changeB.uri}`);
          duplicate_pairs.push([changeA, undefined]);
          pruneA.add(idxA);
        }
      }

      const changeA_current = changesA_current_objects.paths[changeB.target.path]
      if( (changeB.current === undefined && changeA_current === undefined) || (changeB.current && changeB.current.equals(changeA_current)) ) {
        if(!pruneB.has(idxB)) {
          console.log(`${changeB} already in sync on ${changeA.uri}`);
          duplicate_pairs.push([undefined, changeB]);
          pruneB.add(idxB);
        }
      }
    }
  }

  changesA = changesA.filter( (change,idx) => !pruneA.has(idx) );
  changesB = changesB.filter( (change,idx) => !pruneB.has(idx) );

  // Collapse overlapping conflicts
  const prune = new Set();
  const duplicate_conflicts = [];
  for(const [idxA, conflictA] of conflicts.entries()) {
    if(prune.has(idxA)) continue;

    let duplicate_conflict = undefined;
    for(const [idxB, conflictB] of conflicts.entries()) {
      if(idxA === idxB) continue;
      if(prune.has(idxB)) continue;

      if(conflictA.equals(conflictB)) {
        if(duplicate_conflict === undefined) {
          duplicate_conflict = new DuplicateConflictSet(app);
          duplicate_conflict.conflicts.push(conflictA);
          prune.add(idxA);

          duplicate_conflicts.push(duplicate_conflict);
        }

        duplicate_conflict.conflicts.push(conflictB);
        prune.add(idxB);
      }
    }
  }

  conflicts = conflicts.filter( (conflict,idx) => !prune.has(idx) );
  conflicts = conflicts.concat(duplicate_conflicts);

  return [changesA, changesB, duplicate_pairs, conflicts];
}

exports.generate_changes = generate_changes;
exports.relate_objects = relate_objects;
exports.analyze_changes = analyze_changes;
exports.persist_duplicate_pairs = persist_duplicate_pairs;
exports.Modify = Modify;
exports.Add = Add;
exports.Remove = Remove;
