#!/usr/bin/env node
const { argv } = require('yargs');
const csv = require('csv-parser');
const through2 = require('through2');
const process = require('process');
const fs = require('fs-extra');
const path = require('path');

const filterDebugger = require('debug')('Filter');
const writerDebugger = require('debug')('Filter');

const csvFile = argv.infile;
const outFile = argv.outfile;
const outFileMaxEntries = argv.maxlen || null;

if (!csvFile) {
  console.error(`Usage: ${argv.$0} --infile=<filename>`);
  process.exit(1);
}

const emails = new Set();
let processedRowCount = 0;

const responseFilter = {
  field: 'attributes.interaction.response.0.value',
  value: 'Yes',
};

console.info(`Opening file ${csvFile}`);

const recordCounter = through2.obj((row, enc, cb) => {
  processedRowCount += 1;
  cb(null, row);
});

/**
 * @param {Object}        filter
 * @param {String}        filter.field
 * @param {Number|String} filter.name
 * @returns {DestroyableTransform}
 */
const recordFilter = (filter) => {
  let firstLine = true;
  filterDebugger(`Filtering records on ${filter.field} === ${filter.value}`);
  return through2.obj(function (appcuesEvent, enc, cb) { // eslint-disable-line func-names
    if (!emails.has(appcuesEvent.user_id)
    && appcuesEvent[filter.field] === filter.value
    && appcuesEvent.name === 'appcues:form_submitted') {
      this.push(`${firstLine ? '' : ','}${appcuesEvent.user_id}`);
      emails.add(appcuesEvent.user_id);
      firstLine = false;
    }
    cb();
  });
};

/**
 * @param {String}      outFileName
 * @param {Number|null} maxBatchLen
 * @returns {DestroyableTransform}
 */
const batchedWriter = (outFileName, maxBatchLen = null) => {
  if (!maxBatchLen) {
    return fs.createWriteStream(outFileName);
  }
  writerDebugger(`Creating batches of ${maxBatchLen} files`);
  const fileStreams = [];
  let currFileLen = 0;
  let currFileIndex = 0;
  const genFileName = i => outFileName.replace(path.extname(outFileName), `.${i}${path.extname(outFileName)}`);
  let ws = fs.createWriteStream(genFileName(currFileIndex));
  fileStreams.push(ws);
  return through2((chunk, enc, cb) => {
    if (currFileLen > maxBatchLen) {
      currFileIndex += 1;
      currFileLen = 0;
      fileStreams.push(ws = fs.createWriteStream(genFileName(currFileIndex)));
    }
    ws.write(currFileLen === 0 ? chunk.toString().replace(',', '') : chunk); // omit leading comma for new files
    currFileLen += 1;
    cb();
  }, (onFlush) => {
    fileStreams.forEach(s => s.end());
    writerDebugger(`Created ${fileStreams.length} file(s)`);
    onFlush();
  });
};

/**
 * @typedef {Object}       parseOptions
 * @property {String}      parseOptions.outFile
 * @property {Number|null} parseOptions.batchLen
 *
 * @typedef {Object}       outputInfo
 * @property {Number}      outputInfo.total
 * @property {Number}      outputInfo.filtered
 *
 * @param {String}       input
 * @param {parseOptions} opts
 * @returns {Promise.<outputInfo>}
 */
const parse = (input, opts = {}) => {
  const runtimeOptions = Object.assign({}, {
    outFile: 'out.txt',
    batchLen: null,
  }, opts);
  const outputStream = runtimeOptions.outFile ?
    batchedWriter(runtimeOptions.outFile, runtimeOptions.batchLen) : through2((c, e, cb) => { cb(null, c); });
  return new Promise((resolve, reject) => {
    fs.access(input).then(() => {
      fs.createReadStream(csvFile)
        .pipe(csv())
        .pipe(recordCounter)
        .pipe(recordFilter(responseFilter))
        .pipe(outputStream)
        .on('finish', () => resolve({ total: processedRowCount, filtered: emails.size }));
    }).catch(() => reject(new Error(`File '${input}' does not exist`)));
  });
};

(async () => {
  try {
    const { total, filtered } = await parse(csvFile, { outfile: outFile, batchLen: outFileMaxEntries });
    console.info(`Processed a total of ${total} entries`);
    console.info(`${filtered} unique email addresses indicated as Suppliers`);
    process.exit(0);
  } catch (e) {
    console.error(e);
    process.exit(1);
  }
})();
