const path = require('path');

const {join, dirname, PROCEED, ALREADY_APPLIED, HALT} = require('./utils');
const {RFile, RDirectory} = require('./rclone-fs');
const {rclone_load_object, rclone_load_objects, rclone_moveto, rclone_copy, rclone_mkdir} = require('./rclone-ops');

class Conflict {
  constructor (app, changeA, changeB, changesA_current_objects, changesB_current_objects) {
    this.app = app;
    this.changeA = changeA;
    this.changeB = changeB;
    this.changesA_current_objects = changesA_current_objects;
    this.changesB_current_objects = changesB_current_objects;
  }

  toString() {
    return `${this.constructor.name} Conflict - ${this.changeA.type} of ${this.changeA.target.type} ${join(this.changeA.uri, this.changeA.target.path)} ` +
           `with ${this.changeB.type} of ${this.changeB.target.type} ${join(this.changeB.uri, this.changeB.target.path)}.`
  }
}

class DuplicateConflictSet extends Conflict {
  constructor (app) {
    super(app);
    this.applied_changes = [];
    this.conflicts = [];
  }

  async resolve() {
    for(const conflict of this.conflicts.reverse()) {
      const applied_change = conflict.changeToApply();

      if( !(this.applied_changes.find(_ => _.equals(applied_change))) ) {
        await conflict.resolve();
        this.applied_changes.push(applied_change);
      }
    }
  }

  changeToApply() {
    return undefined;
  }

  equals(other_conflict) {
    return false;
  }

  toString() {
    let str = this.conflicts.join("\n  ");
    return `${this.constructor.name} : \n  ${str}`;
  }
}

class RenameB extends Conflict {
  constructor (app, changeA, changeB, changesA_current_objects, changesB_current_objects) {
    super(app, changeA, changeB, changesA_current_objects, changesB_current_objects);

    this.rename_target_uri = changeB.uri;

    if(this.changeB.target.path === this.changeA.target.path) {
      this.rename_target_path = this.changeB.target.path;
      this.rename_type = this.changeB.target.constructor;
    } else if(this.changeB.target.parentOf(this.changeA.target)) {
      this.rename_target_path = this.changeB.target.path;
      this.rename_type = RDirectory;
    } else if(this.changeA.target.parentOf(this.changeB.target)) {
      this.rename_target_path = this.changeA.target.path;
      this.rename_type = RDirectory;
    }
  }

  changeToApply() {
    return this.changeA;
  }

  async newPath() {
    let iter = 0;
    let path_test;
    while(true) {
      if(iter === 0) {
        if(this.rename_type === RDirectory) {
          path_test = `${this.rename_target_path}_conflict`;
        } else {
          const path_obj = path.parse(this.rename_target_path);
          path_test = `${path_obj.dir}/${path_obj.name}_conflict${path_obj.ext}`
        }
      } else {
        if(this.rename_type === RDirectory) {
          path_test = `${this.rename_target_path}_conflict${iter}`;
        } else {
          const path_obj = path.parse(this.rename_target_path);
          path_test = `${path_obj.dir}/${path_obj.name}_conflict${iter}${path_obj.ext}`
        }
      }

      const testA = await rclone_load_object(this.app, this.changeA.uri, path_test);
      const testB = await rclone_load_object(this.app, this.changeB.uri, path_test);
      if(testA === undefined && testB === undefined) {
        break;
      }

      iter++;
    }

    return path_test;
  }

  async persistMoveto(new_path) {
    const moved_objects = await rclone_load_objects(this.app, this.changeB.uri, new_path, this.rename_type);

    const remove_paths = [];
    this.changesB_current_objects.objs = this.changesB_current_objects.objs.filter(obj => {
      if(obj.path === this.rename_target_path || obj.path.startsWith(`${this.rename_target_path}/`)) {
        delete this.changesB_current_objects.paths[obj.path];
        remove_paths.push(obj.path);
        return false;
      } else {
        return true;
      }
    });
    for(const remove_path of remove_paths) {
      await this.changeB.db.del(remove_path);
    }

    for(const obj of moved_objects) {
      this.changesB_current_objects.objs.push(obj);
      this.changesB_current_objects.paths[obj.path] = obj;
      await this.changeB.db.put(obj.path, obj.toJSON());
    }
  }

  async persistCopy(new_path) {
    const copied_objects = await rclone_load_objects(this.app, this.changeA.uri, new_path, this.rename_type);

    for(const obj of copied_objects) {
      this.changesA_current_objects.objs.push(obj);
      this.changesA_current_objects.paths[obj.path] = obj;
      await this.changeA.db.put(obj.path, obj.toJSON());
    }
  }

  async resolve() {
    const statusB = await this.changeB.validateSync();
    const statusA = await this.changeA.validateSync();

    if(statusB === PROCEED && statusA === PROCEED) {
      const new_path = await this.newPath();

      // Results in dirty untracked changes on both ends.  Rely on analyze_changes to deal with this ¯\_(ツ)_/¯
      //   Common case - the skipped change deleted directories that this change quietly recreates
      await rclone_moveto(this.app, join(this.rename_target_uri, this.rename_target_path), join(this.rename_target_uri, new_path));
      await this.persistMoveto(new_path);

      if(this.rename_type === RDirectory) {
        await rclone_mkdir(this.app, join(this.changeA.uri, new_path));
        await rclone_copy(this.app, join(this.rename_target_uri, new_path), join(this.changeA.uri, new_path));
      } else {
        const parent_path = dirname(new_path);
        await rclone_copy(this.app, join(this.rename_target_uri, new_path), join(this.changeA.uri, parent_path));
      }
      await this.persistCopy(new_path);

      await this.changeA.syncTo({local_test_only: true});
    } else if(statusB === ALREADY_APPLIED || statusA === ALREADY_APPLIED) {
      if(statusB === ALREADY_APPLIED) {
        console.log(`Not resolving : ${this.changeB.type} of ${this.changeB.current.path} change already applied on ${this.changeB.remote_uri}`);
      }
      if(statusA === ALREADY_APPLIED) {
        console.log(`Not resolving : ${this.changeA.type} of ${this.changeA.current.path} change already applied on ${this.changeA.remote_uri}`);
      }

    }
  }

  equals(other_conflict) {
    return (other_conflict instanceof RenameB && this.rename_target_uri === other_conflict.rename_target_uri && this.rename_target_path === other_conflict.rename_target_path);
  }
}

class SkipB extends Conflict {

  changeToApply() {
    return this.changeA;
  }

  async resolve() {
    // Results in dirty untracked changes on both ends.  Rely on analyze_changes to deal with this ¯\_(ツ)_/¯
    //   Common case - the skipped change deleted directories that this change quietly recreates
    await this.changeA.syncTo();
  }

  equals(other_conflict) {
    return (other_conflict instanceof SkipB && this.changeA.equals(other_conflict.changeA));
  }
}

class SkipA extends Conflict {

  changeToApply() {
    return this.changeB;
  }

  async resolve() {
    // Results in dirty untracked changes on both ends.  Rely on analyze_changes to deal with this ¯\_(ツ)_/¯
    //   Common case - the skipped change deleted directories that this change quietly recreates
    await this.changeB.syncTo();
  }

  equals(other_conflict) {
    return (other_conflict instanceof SkipA && this.changeB.equals(other_conflict.changeB));
  }
}

exports.DuplicateConflictSet = DuplicateConflictSet;
exports.RenameB = RenameB;
exports.SkipB = SkipB;
exports.SkipA = SkipA;
