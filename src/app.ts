import { ExtendedRequestOptions, requestFile, requestJson } from 'by-request';
import { StatOptions, Stats } from 'fs';
import { readFile, rename, stat, unlink, utimes } from 'fs/promises';
import { noop, processMillis } from '@tubular/util';
import { abs, floor, mod } from '@tubular/math';
import { spawn } from 'child_process';
import filePath from 'path';
import { Feature, FeatureCollection, bbox as getBbox, booleanPointInPolygon } from '@turf/turf';

const THREE_MONTHS = 90 * 86400 * 1000;

const TIMEZONE_RELEASE_URL = 'https://api.github.com/repos/evansiroky/timezone-boundary-builder/releases/latest';
const TIMEZONE_SHAPES_FILE = 'cache/timezone_shapes.zip';
const TIMEZONE_SHAPES_JSON_FILE = 'cache/timezone_shapes.json';

const zoneGrid: Feature[][][] = [];

for (let x = 0; x < 24; ++x) {
  zoneGrid[x] = [];

  for (let y = 0; y < 12; ++y)
    zoneGrid[x][y] = [];
}

function findTimezone(lat: number, lon: number): string {
  const x = floor(mod(lon, 360) / 15);
  const y = floor((lat + 90) / 15);

  for (const zone of zoneGrid[x][y]) {
    if (booleanPointInPolygon([lon, lat], zone.geometry as any))
      return zone.properties.tzid;
  }

  return null;
}

async function safeStat(path: string, opts?: StatOptions & { bigint?: false }): Promise<Stats> {
  try {
    return await stat(path, opts);
  }
  catch {
    return null;
  }
}

async function getPossiblyCachedFile(file: string, url: string, name: string,
                                     extraOpts?: ExtendedRequestOptions): Promise<void> {
  let tickShown = false;
  let lastTick = processMillis();
  const autoTick = setInterval(() => {
    const now = processMillis();

    if (now > lastTick + 500) {
      tickShown = true;
      process.stdout.write('◦');
      lastTick = now;
    }
  }, 1500);
  const opts: ExtendedRequestOptions = { cachePath: file, progress: () => {
    const now = processMillis();

    if (now > lastTick + 500) {
      tickShown = true;
      process.stdout.write('•');
      lastTick = now;
    }
  } };
  const stats = await safeStat(file);

  if (extraOpts)
    Object.assign(opts, extraOpts);

  if (!stats)
    console.log(`Retrieving ${name}`);

  try {
    await requestFile(url, opts, file);
    clearInterval(autoTick);

    if (tickShown)
      process.stdout.write('\n');

    const postStats = await safeStat(file);

    if (stats) {
      if (postStats.mtimeMs > stats.mtimeMs)
        console.log(`Updating ${name}`);
      else
        console.log(`Using cached ${name}`);
    }
  }
  catch (err) {
    console.error(`Failed to acquire ${name}. Will used archived copy.`);
    await readFile(file.replace(/^cache\//, 'archive/'), 'utf-8');
  }
}

async function checkUnzip(): Promise<void> {
  const zipProc = spawn('unzip', ['-h']);

  if (!await new Promise<boolean>(resolve => {
        zipProc.once('error', () => resolve(false));
        zipProc.stdout.once('end', () => resolve(true));
      }))
    throw new Error('unzip command not available');
}

async function getTimezoneShapes(): Promise<FeatureCollection> {
  const releaseInfo = await requestJson(TIMEZONE_RELEASE_URL, { headers: { 'User-Agent': 'GeoDB Updater ' } });
  const asset = releaseInfo?.assets?.find((asset: any) => asset.name === 'timezones-with-oceans.geojson.zip');

  if (!asset.browser_download_url)
    throw new Error('Cannot obtain timezone shapes release info');

  await getPossiblyCachedFile(TIMEZONE_SHAPES_FILE, asset.browser_download_url, 'Timezone shapes',
    { maxCacheAge: THREE_MONTHS });

  const statZip = await safeStat(TIMEZONE_SHAPES_FILE);
  const statJson = await safeStat(TIMEZONE_SHAPES_JSON_FILE);

  if (!statJson || abs(statJson.mtimeMs - statZip.mtimeMs) > 2000) {
    console.log('Unzipping timezone shapes');

    if (statJson)
      await unlink(TIMEZONE_SHAPES_JSON_FILE);

    let zipProc = spawn('unzip', ['-Z1', TIMEZONE_SHAPES_FILE]);
    let unzipName = '';

    await new Promise<void>(resolve => {
      zipProc.once('error', () => resolve());
      zipProc.stdout.on('data', data => unzipName += data.toString());
      zipProc.stdout.once('end', () => resolve());
    });

    unzipName = filePath.join('cache', unzipName.trim());
    await unlink(unzipName).catch(noop);

    zipProc = spawn('unzip', [TIMEZONE_SHAPES_FILE, '-d', 'cache']);

    await new Promise<void>(resolve => {
      zipProc.once('error', () => resolve());
      zipProc.stdout.once('end', () => resolve());
    });

    await rename(unzipName, TIMEZONE_SHAPES_JSON_FILE);
    await utimes(TIMEZONE_SHAPES_JSON_FILE, statZip.mtime, statZip.mtime);
  }
  else
    console.log(`Using cached timezone_shapes.json`);

  const shapesJson = await readFile(TIMEZONE_SHAPES_JSON_FILE, 'utf8');

  return JSON.parse(shapesJson) as FeatureCollection;
}

function presortTimezones(timezones: FeatureCollection): void {
  timezones.features.forEach(shape => {
    const bbox = getBbox(shape);
    let [lonA, latA, lonB, latB] = bbox;

    if (lonA < 0) lonA += 360;
    while (lonB < lonA) lonB += 360;

    const xA = floor(lonA / 15);
    const xB = floor(lonB / 15);
    const yA = floor((latA + 90) / 15);
    const yB = floor((latB + 90) / 15);

    for (let x = xA; x <= xB; ++x) {
      for (let y = yA; y <= yB; ++y)
        zoneGrid[x % 24][y].push(shape);
    }
  });
}

(async (): Promise<void> => {
  try {
    await checkUnzip();

    const timezones = await getTimezoneShapes();

    timezones.features = timezones.features.filter(shape => !shape.properties.tzid.startsWith('Etc/'));
    presortTimezones(timezones);

    console.log(findTimezone(42.7564, -71.4667)); // NYC
    console.log(findTimezone(41.85, -87.65)); // Chicago
    console.log(findTimezone(42.7564, -71.4667)); // Nashua
    console.log(findTimezone(-31.5546, 159.082)); // Lord Howe Island
    console.log(findTimezone(30.0167, 31.2167)); // Giza
    console.log(findTimezone(39.7684, -86.158)); // Indianapolis
  }
  catch (err) {
    console.error(err);
    process.exit(1);
  }
})();
