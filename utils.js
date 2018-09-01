const path = require('path');

const PROCEED = 1;
const ALREADY_APPLIED = 2;
const HALT = -1;

const WINDOW_REGEX = /^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}([.0-9]*)/;

class RTime {
  constructor (time_str) {
    this.time_str = time_str;
    this.window = this.calcWindow();
    this.date = new Date(this.time_str);

  }

  calcWindow() {
    const match = WINDOW_REGEX.exec(this.time_str);
    if(match[1] === '') {
      return 1;
    } else {
      const decimal_length = match[1].length - 1;
      if(decimal_length > 3) {
        return 0.001;
      } else {
        return 1 / Math.pow(10, decimal_length);
      }
    }
  }

  equals(other_time) {
    if(other_time instanceof RUnsupportedTime) {
      return true;
    }

    let compare_window = this.window;
    if(other_time.window > compare_window) {
      compare_window = other_time.window;
    }

    const dt = (this.date.getTime() - other_time.date.getTime()) / 1000;

    return (dt < compare_window && dt > -compare_window);
  }

  toJSON() {
    return this.time_str;
  }
}

class RUnsupportedTime {
  constructor (time_str) {
    this.time_str = time_str;
  }

  equals(other_time) {
    return true;
  }

  toJSON() {
    return this.time_str;
  }
}

function db_next(leveldown_iterator) {
  return new Promise((resolve,reject) => {
    const cb = (err,key,value) => {
      if(err) {
        reject(err);
      } else {
        if(key === undefined && value === undefined) {
          resolve(undefined);
        } else {
          resolve({key, value});
        }
      }
    };
    leveldown_iterator.next(cb);
  });
}

async function* db_iterate(db) {
  const iter = db.iterator();

  try {
    while(true) {
      let value = await db_next(iter);
      if(value === undefined) {
        break;
      }
      yield value;
    }
  } finally {
    iter.end(_ => {});
  }

  return;
}

async function level_get(db, key) {
  let value = undefined;
  try {
    value = await db.get(key);
  } catch(err) {}

  return value;
}

function dirname(full) {
  let name = path.dirname(full);

  if(name === ".") {
    name = "";
  }

  return name;
}

function join(path1, path2) {
  let sep = "/";

  if(path1 === '' || path1.endsWith(':')) {
    sep = '';
  }

  return `${path1}${sep}${path2}`;
}

exports.level_get = level_get;
exports.dirname = dirname;
exports.join = join;
exports.RTime = RTime;
exports.RUnsupportedTime = RUnsupportedTime;
exports.db_iterate = db_iterate;
exports.PROCEED = PROCEED;
exports.ALREADY_APPLIED = ALREADY_APPLIED;
exports.HALT = HALT;
