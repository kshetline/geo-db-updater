import { requestJson } from 'by-request';
import { createReadStream } from 'fs';
import { readFile } from 'fs/promises';
import { regexEscape, toBoolean, toNumber } from '@tubular/util';
import { floor, mod } from '@tubular/math';
import { spawn } from 'child_process';
import { Feature, FeatureCollection, bbox as getBbox, booleanPointInPolygon } from '@turf/turf';
import * as readline from 'readline';
import { Pool, PoolConnection } from './mysql-await-async';
import { admin1s, admin2s, code2ToCode3, countries, initGazetteer, makeKey, processPlaceNames, roughDistanceBetweenLocationsInKm } from './gazetteer';
import { getPossiblyCachedFile, THREE_MONTHS } from './file-util';
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

const ALT_NAMES_URL = 'https://download.geonames.org/export/dump/alternateNames.zip';
const ALT_NAMES_FILE = 'cache/alternateNames.zip';
const ALT_NAMES_TEXT_FILE = 'cache/alternateNames.txt';

const POSTAL_URL = 'https://download.geonames.org/export/zip/allCountries.zip';
const POSTAL_FILE = 'cache/allCountriesPostal.zip';
const POSTAL_TEXT_FILE = 'cache/allCountriesPostal.txt';

interface Location {
  name: string;
  key_name: string;
  geonames_id?: number;
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
  feature_code: string;
  rank: number;
  mphone1?: string;
  mphone2?: string;
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
  const options = { headers: { 'User-Agent': FAKE_USER_AGENT } } as any;
  const gitHubToken = process.env.GITHUB_TOKEN;

  if (gitHubToken)
    options.headers.Authorization = 'token ' + gitHubToken;

  const releaseInfo = await requestJson(TIMEZONE_RELEASE_URL, options);
  const asset = releaseInfo?.assets?.find((asset: any) => asset.name === 'timezones-with-oceans.geojson.zip');

  if (!asset.browser_download_url)
    throw new Error('Cannot obtain timezone shapes release info');

  await getPossiblyCachedFile(TIMEZONE_SHAPES_FILE, asset.browser_download_url, 'Timezone shapes',
    { maxCacheAge: THREE_MONTHS, unzipPath: TIMEZONE_SHAPES_JSON_FILE });

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

let doList = '';

function shouldDo(step: string): boolean {
  return doList === 'all' || new RegExp('\\b' + regexEscape(step) + '\\b').test(doList);
}

async function getGeoData(): Promise<void> {
  await getPossiblyCachedFile(CITIES_15000_FILE, CITIES_15000_URL, 'cities-15000',
    { maxCacheAge: THREE_MONTHS, unzipPath: CITIES_15000_TEXT_FILE });
  await getPossiblyCachedFile(ALL_COUNTRIES_FILE, ALL_COUNTRIES_URL, 'all-countries',
    { maxCacheAge: THREE_MONTHS, unzipPath: ALL_COUNTRIES_TEXT_FILE });
  await getPossiblyCachedFile(ALT_NAMES_FILE, ALT_NAMES_URL, 'alt-names',
    { maxCacheAge: THREE_MONTHS, unzipPath: ALT_NAMES_TEXT_FILE });
  await getPossiblyCachedFile(POSTAL_FILE, POSTAL_URL, 'all-countries-postal',
    { maxCacheAge: THREE_MONTHS, unzipPath: POSTAL_TEXT_FILE });
}

const places: Location[] = [];
const geoNamesLookup = new Map<number, Location>();

async function readGeoData(file: string, level = 0): Promise<void> {
  const inStream = createReadStream(file, 'utf8');
  const lines = readline.createInterface({ input: inStream, crlfDelay: Infinity });

  for await (const line of lines) {
    const parts = line.split('\t');
    const [geonames_id, , , , latitude, longitude, , , , , , , , , population, elevation0, dem] = parts.map(p => toNumber(p, null));
    const [, name, , altNames, , , featureClass, featureCode, countryCode, , admin1, admin2, , , , , , timezone] = parts;
    const elevation = (elevation0 !== -9999 ? elevation0 : 0) || (dem !== -9999 ? dem : 0);
    const feature_code = featureClass + '.' + featureCode;

    if (!name || name.includes(',') || geoNamesLookup.has(geonames_id) || admin1 === '0Z' ||
        !/[PT]/i.test(featureClass) ||
        (featureClass === 'P' && !(population > 1000 || /PPLA|PPLA2|PPLA3|PPLC|PPLG/i.test(featureClass))) ||
        (featureClass === 'P' && level > 0 && /[/()[\]]/.test(name)) ||
        (featureClass === 'T' && !/^(ATOL|CAPE|ISL|ISLET|MT|PK|PT|VLC)$/i.test(featureCode)) ||
        (featureClass === 'T' && /^(((Hill|Number|Peak) \d+)|(\b\d+ (Hill|Islet)))$/i.test(featureCode)) ||
        (featureClass === 'T' && elevation < 600 && /^(MT|PK)$/i.test(featureCode)))
      continue;

    const p = processPlaceNames(name, admin2, admin1, countryCode);

    if (p) {
      let rank = (featureClass === 'T' ? 0 : (level === 0 ? 2 : 1));
      const metaphone = doubleMetaphone(name);

      if (featureClass === 'P') {
        if (feature_code === 'P.PPLC')
          rank += 2;
        else if (feature_code === 'P.PPLA')
          ++rank;
        else if (population === 0)
          --rank;

        if (population >= 1000000 || (feature_code === 'P.PPLC' && population > 500000))
          ++rank;
      }

      const location = {
        name: p.city,
        key_name: makeKey(name),
        geonames_id,
        source: 'GEON',
        variants: altNames.split(','),
        admin2,
        admin1,
        country: p.country,
        latitude,
        longitude,
        elevation,
        population,
        timezone: timezone || findTimezone(latitude, longitude),
        feature_code,
        rank,
        mphone1: metaphone[0],
        mphone2: metaphone[1]
      };

      if (metaphone[1] === metaphone[0])
        delete location.mphone2;

      places.push(location);
      geoNamesLookup.set(geonames_id, location);

      if (places.length % 50000 === 0)
        console.log('size:', places.length);
    }
  }
}

async function updatePrimaryTables(): Promise<void> {
  places.sort((a, b) => a.country === 'USA' && b.country !== 'USA' ? -1 :
    a.country !== 'USA' && b.country === 'USA' ? 1 : a.geonames_id - b.geonames_id);

  let connection: PoolConnection;
  let index = 0, lastPercent = 0;

  try {
    connection = await pool.getConnection();

    for (const loc of countries.values()) {
      const query = `INSERT INTO gazetteer_countries
        (name, key_name, iso2, iso3, geonames_id, postal_regex, source) values (?, ?, ?, ?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE
           name = ?, key_name = ?, iso2 = ?, geonames_id = ?, postal_regex = ?, source = ?, time_stamp = now()`;
      const values = [loc.name, loc.key_name, loc.iso2, loc.iso3, loc.geonames_id, loc.postal_regex, loc.source,
                      loc.name, loc.key_name, loc.iso2, loc.geonames_id, loc.postal_regex, loc.source];

      await connection.queryResults(query, values);

      const percent = floor(++index * 100 / countries.size);

      if (percent > lastPercent) {
        console.log(`countries written: ${percent}%`);
        lastPercent = percent;
      }
    }

    if (lastPercent !== 100)
      console.log('countries written: 100%');

    lastPercent = 0;
    index = 0;

    for (let i = 1; i <= 2; ++i) {
      const admins = (i === 1 ? admin1s : admin2s);

      for (const loc of admins.values()) {
        const query = `INSERT INTO gazetteer_admin${i}
          (name, key_name, code, geonames_id, source) values (?, ?, ?, ?, ?)
           ON DUPLICATE KEY UPDATE
             name = ?, code = ?, geonames_id = ?, source = ?, time_stamp = now()`;
        const values = [loc.name, loc.key_name, loc.code, loc.geonames_id, loc.source,
                        loc.name, loc.code, loc.geonames_id, loc.source];

        await connection.queryResults(query, values);

        const percent = floor(++index * 100 / admins.size);

        if (percent > lastPercent) {
          console.log(`admin${i}s written: ${percent}%`);
          lastPercent = percent;
        }
      }

      if (lastPercent !== 100)
        console.log(`admin${i}s written: 100%`);

      lastPercent = 0;
      index = 0;
    }

    for (const loc of places) {
      const query = `INSERT INTO gazetteer
        (key_name, name, admin2, admin1, country,
         latitude, longitude, elevation, population, rank, feature_code,
         mphone1, mphone2, source, geonames_id) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE
           latitude = ?, longitude = ?, elevation = ?, population = ?, rank = ?, source = ?, geonames_id = ?,
           time_stamp = now()`;
      const values = [loc.key_name, loc.name, loc.admin2, loc.admin1, loc.country,
                      loc.latitude, loc.longitude, loc.elevation, loc.population, loc.rank, loc.feature_code,
                      loc.mphone1, loc.mphone2, loc.source, loc.geonames_id,
                      loc.latitude, loc.longitude, loc.elevation, loc.population, loc.rank, loc.source, loc.geonames_id];

      await connection.queryResults(query, values);

      const percent = floor(++index * 1000 / places.length) / 10;

      if (percent > lastPercent) {
        console.log(`places written: ${percent.toFixed(1)}%`);
        lastPercent = percent;
      }
    }

    if (lastPercent !== 100)
      console.log('places written: 100.0%');
  }
  catch (err) {
    console.error(err.toString());
  }

  connection?.release();
}

async function processAltNames(): Promise<void> {
  let connection: PoolConnection;

  try {
    connection = await pool.getConnection();
    const inStream = createReadStream(ALT_NAMES_TEXT_FILE, 'utf8');
    const lines = readline.createInterface({ input: inStream, crlfDelay: Infinity });
    const idMap: Map<number, { geonames_id: number, id: number, name: string }>[] = [];
    let index = 0;

    for await (const line of lines) {
      const parts = line.split('\t').map(p => p.trim());
      const [geonames_alt_id, geonames_orig_id, , , preferred, short, colloquial, historic] = parts.map(p => toNumber(p));
      const [, , lang, name] = parts;
      const key_name = makeKey(name);

      let type = '';
      let gazetteer_id = 0;
      let origName = '';
      const tables = key_name && lang.length < 3 ?
        ['gazetteer', 'gazetteer_admin2', 'gazetteer_admin1', 'gazetteer_countries'] : [];

      for (let i = 0; i < tables.length; ++i) {
        if (idMap[i] == null) {
          const query = `SELECT id, name, geonames_id FROM ${tables[i]} WHERE 1`;
          const result = await connection.queryResults(query, [geonames_orig_id]);

          idMap[i] = new Map();

          if (result) {
            result.forEach((row: any) => idMap[i].set(row.geonames_id, row));
            console.log('populated id map for %s', tables[i]);
          }
        }

        const match = idMap[i].get(geonames_orig_id);

        if (match) {
          type = 'P21C'.charAt(i);
          gazetteer_id = match.id;
          origName = match.name;
          break;
        }
      }

      if (type && gazetteer_id && origName !== name) {
        const query = `INSERT INTO gazetteer_alt_names
          (name, lang, key_name, geonames_alt_id, geonames_orig_id, gazetteer_id, type, source,
           preferred, short, colloquial, historic, misspelling) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON DUPLICATE KEY UPDATE
             name = ?, geonames_alt_id = ?, geonames_orig_id = ?, source = ?, time_stamp = now()`;
        const values = [name, lang, key_name, geonames_alt_id, geonames_orig_id, gazetteer_id, type, 'GEON',
                        preferred, short, colloquial, historic, 0,
                        name, geonames_alt_id, geonames_orig_id, 'GEON'];

        await connection.queryResults(query, values);
      }

      if (++index % 10000 === 0)
        console.log(`${index} alternate names processed`);
    }
  }
  catch (err) {
    console.error(err.toString());
  }

  connection?.release();
}

async function findTimezoneInDb(connection: PoolConnection, lat: number, lon: number): Promise<string> {
  zoneLoop:
  for (const span of [0.05, 0.1, 0.25, 0.5]) {
    const query = `SELECT timezone FROM gazetteer
                     WHERE latitude >= ? AND latitude <= ? AND longitude >= ? AND longitude <= ?`;
    const results = (await connection.queryResults(query, [lat - span, lat + span, lon - span, lon + span])) || [];
    let timeZoneId: string;
    let country: string;

    for (const result of results) {
      if (result.time_zone) {
        if (!timeZoneId)
          timeZoneId = result.time_zone;
        else if (timeZoneId !== result.time_zone)
          break zoneLoop;

        if (!country)
          country = result.country;
        else if (country !== result.country)
          break zoneLoop;
      }
    }

    if (timeZoneId)
      return timeZoneId;
  }

  return null;
}

async function processPostalCodes(): Promise<void> {
  let connection: PoolConnection;

  try {
    connection = await pool.getConnection();
    const inStream = createReadStream(POSTAL_TEXT_FILE, 'utf8');
    const lines = readline.createInterface({ input: inStream, crlfDelay: Infinity });
    let index = 0;

    for await (const line of lines) {
      const parts = line.split('\t').map(p => p.trim());
      const [country, code, name, , admin1] = parts;
      let [latitude, longitude, accuracy] = parts.slice(9).map(p => toNumber(p));
      const iso3 = code2ToCode3[country];
      let geonames_id = 0;
      let gazetteer_id = 0;
      let timezone = '';

      let query = `SELECT id, geonames_id, timezone, latitude, longitude, source
                     FROM gazetteer WHERE name = ? AND country = ? AND admin1 = ?
                     AND ABS(? - latitude) < 0.25 AND ABS(? - longitude) < 0.25`;
      let values: any[] = [name, iso3, admin1, latitude, longitude];
      let result = await connection.queryResults(query, values);
      let alreadyCopied = false;

      if (!result || result.length === 0) {
        query = `SELECT id, geonames_id, timezone, latitude, longitude FROM gazetteer
                   WHERE geonames_id IN (SELECT geonames_orig_id FROM gazetteer_alt_names WHERE name = ?) AND
                     country = ? AND admin1 = ? AND ABS(? - latitude) < 0.25 AND ABS(? - longitude) < 0.25`;
        result = await connection.queryResults(query, values);
      }
      else {
        for (const loc of result) {
          if (loc.name === name && loc.source === 'GEOZ' && loc.geonames_id === 0) {
            alreadyCopied = true;
            break;
          }
        }
      }

      if (result?.length > 0) {
        geonames_id = result[0].geonames_id;
        gazetteer_id = result[0].id;
        timezone = result[0].timezone;

        if (accuracy < 4) {
          latitude = result[0].latitude;
          longitude = result[0].longitude;
        }
      }

      if (!timezone)
        timezone = await findTimezoneInDb(connection, latitude, longitude);

      if (timezone) {
        if (!alreadyCopied && geonames_id === 0 && gazetteer_id === 0) {
          const metaphone = doubleMetaphone(name);

          if (metaphone[1] === metaphone[0])
            metaphone[1] = null;

          query = `INSERT INTO gazetteer
            (key_name, name, admin2, admin1, country, latitude, longitude, elevation, population, rank, feature_code,
             mphone1, mphone2, source, geonames_id) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
          values = [makeKey(name), name, null, admin1, country, latitude, longitude, 0, 0, 1, 'P.PPL',
                    metaphone[0], metaphone[1], 'GEOZ', 0];
          await connection.queryResults(query, values);
          gazetteer_id = toNumber((await connection.queryResults(`SELECT LAST_INSERT_ID()`) || [])[0]);
        }

        query = `INSERT INTO gazetteer_postal
                   (country, code, name, admin1, latitude, longitude, accuracy,
                    timezone, geonames_id, gazetteer_id, source) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE
                      name = ?, latitude = ?, longitude = ?, accuracy = ?,
                      timezone = ?, geonames_id = ?, gazetteer_id = ?, source = ?, time_stamp = now()`;
        values = [country, code, name, admin1, latitude, longitude, accuracy,
                  timezone, geonames_id, gazetteer_id, 'GEON',
                  name, latitude, longitude, accuracy, timezone, geonames_id, gazetteer_id, 'GEON'];
        await connection.queryResults(query, values);
      }

      if (++index % 10000 === 0)
        console.log(`${index} postal codes processed`);
    }
  }
  catch (err) {
    console.error(err.toString());
  }

  connection?.release();
}

async function legacyItems(): Promise<void> {
  let connection: PoolConnection;

  try {
    connection = await pool.getConnection();
    const oldItems = await connection.queryResults('SELECT * FROM atlas2 where `source` IN (4, 5, 6, 99, 104)');

    if (oldItems?.length > 0) {
      for (const row of oldItems) {
        if (/\b(Addition|Acres|Area|Camp|Campground|Church|Country Club|Division|Estates|Farms|Gardens|Park|Place|Subdivision|Trailer)\b/i.test(row.name))
          continue;

        const query = `SELECT * FROM atlas2 WHERE item_no != ? AND ABS(? - latitude) < 0.25 AND ABS(? - longitude) < 0.25`;
        const values = [row.item_no, row.latitude, row.longitude];
        const results = ((await connection.queryResults(query, values)) || []).filter((loc: any) =>
          roughDistanceBetweenLocationsInKm(row.latitude, row.longitude, loc.latitude, loc.longitude) <= 3 &&
          row.feature_type.charAt(0) === loc.feature_type.charAt(0));

        if (results.length > 0) {
          console.log(row.name, row.admin1 || '-', row.country, ': close neighbors:', results.length);
          console.log('   ', results.map((loc: any) => loc.name).join('; '));
        }
      }
    }
  }
  catch (err) {
    console.error(err.toString());
  }

  connection?.release();
}

(async (): Promise<void> => {
  doList = process.argv.find(arg => arg.startsWith('--do='))?.substring(5) || '';

  if (doList === 'std')
    doList = 'shapes;main;alt;postal';

  try {
    await checkUnzip();

    if (shouldDo('shapes')) {
      const timezones = await getTimezoneShapes();

      timezones.features = timezones.features.filter(shape => !shape.properties.tzid.startsWith('Etc/'));
      presortTimezones(timezones);
    }

    await getGeoData();
    await initGazetteer();

    if (shouldDo('main')) {
      await readGeoData(CITIES_15000_TEXT_FILE);
      await readGeoData(ALL_COUNTRIES_TEXT_FILE, 1);
      await updatePrimaryTables();
    }

    if (shouldDo('alt'))
      await processAltNames();

    if (shouldDo('postal'))
      await processPostalCodes();

    if (shouldDo('legacy'))
      await legacyItems();

    process.exit(0);
  }
  catch (err) {
    console.error(err);
    process.exit(1);
  }
})();
