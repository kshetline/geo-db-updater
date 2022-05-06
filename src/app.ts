import { requestJson } from 'by-request';
import { createReadStream, createWriteStream } from 'fs';
import { readFile } from 'fs/promises';
import { toBoolean, toNumber } from '@tubular/util';
import { floor, mod } from '@tubular/math';
import { spawn } from 'child_process';
import { Feature, FeatureCollection, bbox as getBbox, booleanPointInPolygon } from '@turf/turf';
import * as readline from 'readline';
import { Pool } from './mysql-await-async';
import { initGazetteer, processPlaceNames } from './gazetteer';
import { getPossiblyCachedFile, THREE_MONTHS } from './file-util';
import unidecode from 'unidecode-plus';
import { doubleMetaphone } from 'double-metaphone';

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

/* @ts-ignore */ // eslint-disable-next-line @typescript-eslint/no-unused-vars
function findTimezone(lat: number, lon: number): string {
  const x = floor(mod(lon, 360) / 15);
  const y = floor((lat + 90) / 15);

  for (const zone of zoneGrid[x][y]) {
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

async function readGeoData(file: string): Promise<void> {
  await initGazetteer();

  const inStream = createReadStream(file, 'utf8');
  const outStream = createWriteStream('temp.txt', 'utf8');
  const lines = readline.createInterface({ input: inStream, crlfDelay: Infinity });

  for await (const line of lines) {
    const parts = line.split('\t');
    const [, name, altNames, , , featureClass, featureCode, countryCode, , admin1, admin2, , , , , , timezone] = parts;

    if (!name)
      continue;

    const [geonamesId, , , , latitude, longitude, , , , , , , , , population, elevation] = parts.map(p => toNumber(p, null));
    const p = processPlaceNames(name, admin2, admin1, countryCode);
    const metaphone = doubleMetaphone(name);

    if (p) {
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
        timezone,
        featureCode: featureClass + '.' + featureCode,
        rank: 0,
        metaphone1: metaphone[0],
        metaphone2: metaphone[1]
      };

      places.push(location);
      geoNamesLookup.set(geonamesId, location);
      outStream.write(JSON.stringify(location).replace(/,/g, ', ') + '\n');
    }
  }

  console.log(places.length);
}

(async (): Promise<void> => {
  try {
    await checkUnzip();

    if (Date.now() < 0) {
      const timezones = await getTimezoneShapes();

      timezones.features = timezones.features.filter(shape => !shape.properties.tzid.startsWith('Etc/'));
      presortTimezones(timezones);
    }

    await getGeoData();
    await readGeoData(CITIES_15000_TEXT_FILE);
  }
  catch (err) {
    console.error(err);
    process.exit(1);
  }
})();
