exports.nav = (obj, path, fn, ret = {}, ...params) => {

  if(!path.length)
    return fn(...params, obj, ret);

  if(typeof obj != 'object')
    return;

  let keys = Object.keys(obj);
  if(typeof path[0] == 'string')
    keys = keys.filter(key => key == path[0]);
  else
    keys = keys.filter(key => path[0].test(key));

  keys.forEach(key => exports.nav(obj[key], path.slice(1), fn, ret, ...params, key));

  return ret;

}

const path = [];

function serializedFn(segment, assetType, profitType, profitType) {
  return [ assetType, profitType ];
}

function fn(...params) {
  let ret = params.pop();
  let obj = params.pop();
  let retPath = serializedFn(...params);
  Obj.addAt(ret, retPath, typeof obj == 'object' ? Obj.sum(obj) : obj);
}

Obj.nav(obj, path, fn);
