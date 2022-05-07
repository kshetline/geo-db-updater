import { requestJson } from 'by-request';
import { createReadStream } from 'fs';
import { readFile } from 'fs/promises';
import { toBoolean, toNumber } from '@tubular/util';
import { floor, mod } from '@tubular/math';
import { spawn } from 'child_process';
import { Feature, FeatureCollection, bbox as getBbox, booleanPointInPolygon } from '@turf/turf';
import * as readline from 'readline';
import { Pool, PoolConnection } from './mysql-await-async';
import { initGazetteer, processPlaceNames } from './gazetteer';
import { getPossiblyCachedFile, THREE_MONTHS } from './file-util';
import unidecode from 'unidecode-plus';
import { doubleMetaphone } from './double-metaphone';

const FAKE_USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:98.0) Gecko/20100101 Firefox/98.0';

const TIMEZONE_RELEASE_URL = 'https://api.github.com/repos/evansiroky/timezone-boundary-builder/releases/latest';
const TIMEZONE_SHAPES_FILE = 'cache/timezone_shapes.zip';
const TIMEZONE_SHAPES_JSON_FILE = 'cache/timezone_shapes.json';

const CITIES_15000_URL = 'https://download.geonames.org/export/dump/cities15000.zip';
const CITIES_15000_FILE = 'cache/cities15000.zip';
const CITIES_15000_TEXT_FILE = 'cache/cities15000.txt';

const ALL_COUNTRIES_URL = 'https://download.geonames.org/export/dump/allCountries.zip';
const ALL_COUNTRIES_FILE = 'cache/all-countries.zip';
const ALL_COUNTRIES_TEXT_FILE = 'cache/all-countries.txt';

interface Location {
  name: string;
  key: string;
  geonamesId?: number;
  source?: string;
  variants?: string[];
  admin2?: string;
  admin1?: string;
  country: string;
  latitude: number;
  longitude: number;
  elevation: number;
  population: number;
  timezone: string;
  featureCode: string;
  rank: number;
  metaphone1?: string;
  metaphone2?: string;
}

export const pool = new Pool({
  host: (toBoolean(process.env.DB_REMOTE) ? 'skyviewcafe.com' : '127.0.0.1'),
  user: 'skyview',
  password: process.env.DB_PWD,
  database: 'skyviewcafe'
});

const zoneGrid: Feature[][][] = [];

for (let x = 0; x < 24; ++x) {
  zoneGrid[x] = [];

  for (let y = 0; y < 12; ++y)
    zoneGrid[x][y] = [];
}

function makeKey(name: string): string {
  return unidecode(name, { german: true }).toUpperCase().replace(/[^A-Z]+/g, '');
}

function findTimezone(lat: number, lon: number): string {
  const x = floor(mod(lon, 360) / 15);
  const y = floor((lat + 90) / 15);

  for (const zone of (zoneGrid[x][y] ?? [])) {
    if (booleanPointInPolygon([lon, lat], zone.geometry as any))
      return zone.properties.tzid;
  }

  return null;
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
  const releaseInfo = await requestJson(TIMEZONE_RELEASE_URL, { headers: { 'User-Agent': FAKE_USER_AGENT } });
  const asset = releaseInfo?.assets?.find((asset: any) => asset.name === 'timezones-with-oceans.geojson.zip');

  if (!asset.browser_download_url)
    throw new Error('Cannot obtain timezone shapes release info');

  await getPossiblyCachedFile(TIMEZONE_SHAPES_FILE, asset.browser_download_url, 'Timezone shapes',
    { maxCacheAge: THREE_MONTHS, unzipName: TIMEZONE_SHAPES_JSON_FILE });

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

async function getGeoData(): Promise<void> {
  await getPossiblyCachedFile(CITIES_15000_FILE, CITIES_15000_URL, 'cities-15000',
    { maxCacheAge: THREE_MONTHS, unzipName: CITIES_15000_TEXT_FILE });
  await getPossiblyCachedFile(ALL_COUNTRIES_FILE, ALL_COUNTRIES_URL, 'all-countries',
    { maxCacheAge: THREE_MONTHS, unzipName: ALL_COUNTRIES_TEXT_FILE });
}

const places: Location[] = [];
const geoNamesLookup = new Map<number, Location>();

async function readGeoData(file: string, level = 0): Promise<void> {
  const inStream = createReadStream(file, 'utf8');
  const lines = readline.createInterface({ input: inStream, crlfDelay: Infinity });

  for await (const line of lines) {
    const parts = line.split('\t');
    const [geonamesId, , , , latitude, longitude, , , , , , , , , population, elevation0, dem] = parts.map(p => toNumber(p, null));
    const [, name, , altNames, , , featureClass, featureCode0, countryCode, , admin1, admin2, , , , , , timezone] = parts;
    const elevation = (elevation0 !== -9999 ? elevation0 : 0) || (dem !== -9999 ? dem : 0);
    const featureCode = featureClass + '.' + featureCode0;

    if (!name || name.includes(',') || geoNamesLookup.has(geonamesId) || admin1 === '0Z' ||
        !/[PT]/i.test(featureClass) ||
        (featureClass === 'P' && !(population > 1000 || /PPLA|PPLA2|PPLA3|PPLC|PPLG/i.test(featureClass))) ||
        (featureClass === 'T' && !/^(ATOL|CAPE|ISL|ISLET|MT|PK|PT|VLC)$/i.test(featureCode0)) ||
        (featureClass === 'T' && elevation < 600 && /^(MT|PK)$/i.test(featureCode0)))
      continue;

    const p = processPlaceNames(name, admin2, admin1, countryCode);

    if (p) {
      let rank = (featureClass === 'T' ? 0 : (level === 0 ? 2 : 1));
      const metaphone = doubleMetaphone(name);

      if (featureClass === 'P') {
        if (featureCode === 'PPLC')
          rank += 2;
        else if (featureCode === 'PPLA')
          ++rank;
        else if (population === 0)
          --rank;

        if (population >= 1000000)
          ++rank;
      }

      const location = {
        name: p.city,
        key: makeKey(name),
        geonamesId,
        source: 'GEON',
        variants: altNames.split(','),
        admin2: p.county,
        admin1: p.state,
        country: p.country,
        latitude,
        longitude,
        elevation,
        population,
        timezone: timezone || findTimezone(latitude, longitude),
        featureCode,
        rank,
        metaphone1: metaphone[0],
        metaphone2: metaphone[1]
      };

      if (metaphone[1] === metaphone[0])
        delete location.metaphone2;

      places.push(location);
      geoNamesLookup.set(geonamesId, location);

      if (places.length % 50000 === 0)
        console.log('size:', places.length);
    }
  }
}

async function updatePrimaryTable(): Promise<void> {
  places.sort((a, b) => a.country === 'USA' && b.country !== 'USA' ? -1 :
    a.country !== 'USA' && b.country === 'USA' ? 1 : a.geonamesId - b.geonamesId);

  let connection: PoolConnection;
  let index = 0, lastPercent = 0;

  try {
    connection = await pool.getConnection();

    for (const loc of places) {
      const query = `INSERT INTO gazetteer
        (key_name, name, admin2, admin1, country,
         latitude, longitude, elevation, rank, feature_type,
         mphone1, mphone2, source, geonames_id) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE
           latitude = ?, longitude = ?, elevation = ?, rank = ?, source = ?, geonames_id = ?`;
      const values = [loc.key, loc.name, loc.admin2, loc.admin1, loc.country,
                      loc.latitude, loc.longitude, loc.elevation, loc.rank, loc.featureCode,
                      loc.metaphone1, loc.metaphone2, loc.source, loc.geonamesId,
                      loc.latitude, loc.longitude, loc.elevation, loc.rank, loc.source, loc.geonamesId];

      await connection.queryResults(query, values);

      const percent = Math.floor(++index * 100 / places.length);

      if (percent > lastPercent) {
        console.log(`written: ${percent}%`);
        lastPercent = percent;
      }
    }
  }
  catch (err) {
    console.error(err.toString());
  }

  connection?.release();
}

(async (): Promise<void> => {
  try {
    await checkUnzip();

    if (toBoolean(process.env.DB_GET_TIMEZONE_SHAPES)) {
      const timezones = await getTimezoneShapes();

      timezones.features = timezones.features.filter(shape => !shape.properties.tzid.startsWith('Etc/'));
      presortTimezones(timezones);
    }

    await getGeoData();
    await initGazetteer();
    await readGeoData(CITIES_15000_TEXT_FILE);
    await readGeoData(ALL_COUNTRIES_TEXT_FILE, 1);
    await updatePrimaryTable();
  }
  catch (err) {
    console.error(err);
    process.exit(1);
  }
})();
