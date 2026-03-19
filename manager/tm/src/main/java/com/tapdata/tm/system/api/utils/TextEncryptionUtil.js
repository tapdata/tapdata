'use strict';

function outputTypeBy(code) {
  if (code === 1 || code === 'CUSTOM') return 'CUSTOM';
  return 'AUTO';
}

function replace(rule, text) {
  const outputType = outputTypeBy(rule?.outputType);
  const regexText = rule?.regex;
  if (regexText === null || regexText === undefined) return text;

  let reg;
  try {
    reg = new RegExp(String(regexText), 'g');
  } catch {
    return text;
  }

  const outputChar = rule?.outputChar;
  const outputCount = rule?.outputCount;
  const replaceWith = outputType === 'CUSTOM'
    ? String(outputChar == null ? 'null' : outputChar).repeat(
      Number.isFinite(outputCount) && outputCount > 0 ? Math.floor(outputCount) : 1
    )
    : String(outputChar == null ? 'null' : outputChar);

  return String(text).replace(reg, replaceWith);
}

function tryBackNumber(value, originValue) {
  const n = Number(value);
  if (Number.isNaN(n)) return value;
  if (typeof originValue === 'number') return n;
  return n;
}

function mapFieldValue(textEncryptionRules, value) {
  if (value === null || value === undefined) return null;

  let target;
  let type;

  if (typeof value === 'string') {
    target = value;
    type = 0;
  } else if (Array.isArray(value)) {
    return value.map((item) => mapFieldValue(textEncryptionRules, item));
  } else if (value instanceof Date) {
    target = value.toString();
    type = 30;
  } else if (typeof value === 'object') {
    const keys = Object.keys(value);
    const targetMap = {};
    keys.forEach((k) => {
      targetMap[k] = mapFieldValue(textEncryptionRules, value[k]);
    });
    return targetMap;
  } else if (typeof value === 'number') {
    target = String(value);
    type = 21;
  } else if (typeof value === 'boolean') {
    target = String(value);
    type = 20;
  } else {
    return value;
  }

  let encrypted = target;
  if (Array.isArray(textEncryptionRules)) {
    textEncryptionRules.forEach((rule) => {
      encrypted = replace(rule, encrypted);
    });
  }

  if (type === 21) return tryBackNumber(encrypted, value);
  return encrypted;
}

function findRecursive(current, parent, fields, index, results) {
  if (current === null || current === undefined) return;

  if (index === fields.length) {
    results.push({
      parent,
      value: current,
      key: fields[fields.length - 1]
    });
    return;
  }

  const key = fields[index];

  if (Array.isArray(current)) {
    current.forEach((element) => {
      findRecursive(element, parent, fields, index, results);
    });
    return;
  }

  if (typeof current === 'object') {
    findRecursive(current[key], current, fields, index + 1, results);
  }
}

function deepSearch(fieldPath, value, config) {
  if (!value || !Array.isArray(config) || config.length === 0) return;
  const results = [];
  findRecursive(value, null, fieldPath, 0, results);

  results.forEach((result) => {
    const parent = result.parent;
    const key = result.key;
    if (!parent || typeof parent !== 'object' || Array.isArray(parent)) return;
    const mappedValue = mapFieldValue(config, result.value);
    if (mappedValue === null || mappedValue === undefined) return;
    parent[key] = mappedValue;
  });
}

function map(config, target) {
  if (!target) return null;

  if (target && typeof target === 'object' && !Array.isArray(target) && Array.isArray(target.data)) {
    map(config, target.data);
    return target;
  }

  const data = target;
  if (!config || Object.keys(config).length === 0 || !Array.isArray(data) || data.length === 0) return data;

  Object.keys(config).forEach((fieldName) => {
    const rules = config[fieldName];
    const fieldPath = String(fieldName).split('.');
    data.forEach((item) => deepSearch(fieldPath, item, rules));
  });

  return data;
}

module.exports = {
  map,
  mapFieldValue,
  tryBackNumber,
  replace,
  deepSearch,
  findRecursive
};
