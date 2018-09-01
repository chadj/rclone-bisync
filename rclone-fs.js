const {RTime, RUnsupportedTime} = require('./utils');

class RObject {
  constructor (obj) {
    this.path = obj.Path;
    this.name = obj.Name;
    this.size = obj.Size;
    this.mimetype = obj.MimeType;
    this.isdir = obj.IsDir;
    if(obj.ModTime) {
      this.rtime = new RTime(obj.ModTime);
    } else {
      this.rtime = new RUnsupportedTime(obj.ModTime);
    }
    this.related = undefined;
  }

  parentOf(other_object) {
    return other_object.path.startsWith(`${this.path}/`);
  }

  toJSON() {
    return {
      Path: this.path,
      Name: this.name,
      Size: this.size,
      MimeType: this.mimetype,
      IsDir: this.isdir,
      ModTime: this.rtime.toJSON(),
    };
  }

  toString() {
    return `${this.constructor.name} {path: ${this.path}, isdir: ${this.isdir}}`;
  }
}

class RFile extends RObject {
  get type() {
    return "file";
  }

  equals(other_object) {
    if(other_object === undefined) {
      return false;
    }

    return (other_object.constructor === RFile && this.path === other_object.path && this.size === other_object.size && this.rtime.equals(other_object.rtime));
  }
}

class RDeletedFile extends RFile {
  get type() {
    return "deleted file";
  }

  equals(other_object) {
    if(other_object === undefined) {
      return true;
    }

    return (other_object.constructor === RDeletedFile && this.path === other_object.path);
  }
}

class RDirectory extends RObject {
  get type() {
    return "directory";
  }

  equals(other_object) {
    if(other_object === undefined) {
      return false;
    }

    return (other_object.constructor === RDirectory && this.path === other_object.path);
  }
}

class RDeletedDirectory extends RDirectory {
  get type() {
    return "deleted directory";
  }

  equals(other_object) {
    if(other_object === undefined) {
      return true;
    }

    return (other_object.constructor === RDeletedDirectory && this.path === other_object.path);
  }
}

function fsobject_creator(raw_object) {
  if(raw_object.IsDir) {
    return new RDirectory(raw_object);
  } else {
    return new RFile(raw_object);
  }
}

exports.fsobject_creator = fsobject_creator;
exports.RFile = RFile;
exports.RDirectory = RDirectory;
exports.RDeletedDirectory = RDeletedDirectory;
exports.RDeletedFile = RDeletedFile;
