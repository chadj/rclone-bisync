const child_process = require('child_process');
const util = require('util');
const path = require('path');
const execFile = util.promisify(child_process.execFile);

const {db_iterate, dirname, join} = require('./utils');
const {fsobject_creator, RFile, RDirectory} = require('./rclone-fs');

const MAX_BUFFER = 32 * 1024 * 1024;

function options(app, args, opts) {
  opts = opts ? opts : {};

  if(opts.include) {
    args.splice(1, 0, "--include", opts.include);
  }
  if(app.argv['fast-list']) {
    args.splice(1, 0, "--fast-list");
  }
  if(app.argv['drive-skip-gdocs']) {
    args.splice(1, 0, "--drive-skip-gdocs");
  }
  if(app.argv['filter-from'] !== undefined && opts.ignore_filters !== true) {
    args.splice(1, 0, `--filter-from=${app.argv['filter-from']}`);
  }

  return args;
}

async function load_previous_objects(db) {
  const object_set = {
    paths: {},
    objs: []
  };
  for await (const {key, value} of db_iterate(db)) {
    const r = fsobject_creator(value);
    object_set.paths[r.path] = r;
    object_set.objs.push(r);
  }
  return object_set;
}

async function rclone_load_objects(app, uri, obj_path, type) {
  const objects = [];

  if(type === RDirectory) {
    const name = path.basename(obj_path);
    const parent_path = dirname(obj_path);
    const indirect_objects = (await rclone_lsjson(app, join(uri, parent_path), {ignore_filters: true, recurse: false, include: `${name}/`})).objs;

    for(const obj of indirect_objects) {
      obj.path = join(parent_path, obj.path);
      objects.push(obj);
    }
  }

  const direct_objects = (await rclone_lsjson(app, join(uri, obj_path), {ignore_filters: true})).objs;
  let direct_parent_path = (type === RDirectory) ? obj_path : dirname(obj_path)
  for(const obj of direct_objects) {
    obj.path = join(direct_parent_path, obj.path);
    objects.push(obj);
  }

  return objects;
}

async function rclone_load_object(app, uri, obj_path) {
  let object = undefined;

  let name = path.basename(obj_path);
  let parent_path = dirname(obj_path);

  try {
    //console.log(`Command: rclone lsjson ${join(uri, obj_path)}`);
    const objects = await rclone_lsjson(app, join(uri, obj_path), {ignore_filters: true, recurse: false});
    object = objects.paths[name];
    if(object) {
      object.path = join(parent_path, object.path);
    }
  } catch(err) {}

  if(object === undefined) {
    try {
      //console.log(`Command: rclone lsjson --include ${name}/ ${join(uri, parent_path)}`);
      const objects = await rclone_lsjson(app, join(uri, parent_path), {ignore_filters: true, recurse: false, include: `${name}/`});
      object = objects.paths[name];
      if(object) {
        object.path = join(parent_path, object.path);
      }
    } catch(err) {}
  }

  return object;
}

async function rclone_lsjson(app, uri, opts) {
  opts = opts ? opts : {};

  if(!('recurse' in opts)) {
    opts.recurse = true;
  }

  const args = ['lsjson'];
  if(opts.recurse) {
    args.push('-R');
  }
  args.push(uri);

  const cmd_output = await execFile("rclone", options(app, args, opts), {maxBuffer: MAX_BUFFER});
  const raw_objects = JSON.parse(cmd_output.stdout);

  const object_set = {
    paths: {},
    objs: []
  };
  for(const _ of raw_objects) {
    const r = fsobject_creator(_);
    if(r.path in object_set.paths) {
      throw new Error(`Multiple objects at identical paths: ${r.path}`);
    }
    object_set.paths[r.path] = r;
    object_set.objs.push(r);
  }
  return object_set;
}

async function rclone_touch(app, uri) {
  console.log(`Command: rclone touch ${uri}`);
  const cmd_output = await execFile("rclone", options(app, ["touch", uri], {ignore_filters: true}), {maxBuffer: MAX_BUFFER});
  return true;
}

async function rclone_copy(app, uri1, uri2) {
  console.log(`Command: rclone copy ${uri1} ${uri2}`);
  const cmd_output = await execFile("rclone", options(app, ["copy", uri1, uri2], {ignore_filters: true}), {maxBuffer: MAX_BUFFER});
  return true;
}

async function rclone_mkdir(app, uri) {
  console.log(`Command: rclone mkdir ${uri}`);
  const cmd_output = await execFile("rclone", options(app, ["mkdir", uri], {ignore_filters: true}), {maxBuffer: MAX_BUFFER});
  return true;
}

async function rclone_rmdir(app, uri) {
  console.log(`Command: rclone rmdir ${uri}`);
  const cmd_output = await execFile("rclone", options(app, ["rmdir", uri], {ignore_filters: true}), {maxBuffer: MAX_BUFFER});
  return true;
}

async function rclone_deletefile(app, uri) {
  console.log(`Command: rclone deletefile ${uri}`);
  const cmd_output = await execFile("rclone", options(app, ["deletefile", uri], {ignore_filters: true}), {maxBuffer: MAX_BUFFER});
  return true;
}

async function rclone_moveto(app, uri1, uri2) {
  console.log(`Command: rclone moveto  ${uri1} ${uri2}`);
  const cmd_output = await execFile("rclone", options(app, ["moveto", uri1, uri2], {ignore_filters: true}), {maxBuffer: MAX_BUFFER});
  return true;
}

async function empty_dir_placeholder_maintain(app, uri) {
  const all_objects = await rclone_lsjson(app, uri);

  for(const obj of all_objects.objs) {
    if(obj.isdir && !all_objects.objs.some(_ => obj.parentOf(_))) {
      const rclone_keep = `${obj.path}/.rclone_keep`;
      await rclone_touch(app, join(uri, rclone_keep));
    }
  }

  for(const obj of all_objects.objs) {
    if(!obj.isdir && obj.name === ".rclone_keep") {
      const parent_path = all_objects.paths[dirname(obj.path)];
      const children = all_objects.objs.filter(_ => parent_path.parentOf(_));
      if(children.length > 1) {
        await rclone_deletefile(app, join(uri, obj.path));
      }
    }
  }
}

exports.rclone_lsjson = rclone_lsjson;
exports.rclone_touch = rclone_touch;
exports.rclone_copy = rclone_copy;
exports.rclone_mkdir = rclone_mkdir;
exports.rclone_rmdir = rclone_rmdir;
exports.rclone_deletefile = rclone_deletefile;
exports.rclone_moveto = rclone_moveto;
exports.rclone_load_object = rclone_load_object;
exports.rclone_load_objects = rclone_load_objects;
exports.load_previous_objects = load_previous_objects;
exports.empty_dir_placeholder_maintain = empty_dir_placeholder_maintain;
