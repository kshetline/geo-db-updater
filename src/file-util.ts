import { ExtendedRequestOptions, requestFile, ResponseInfo } from 'by-request';
import { noop, processMillis } from '@tubular/util';
import { readFile, rename, stat, unlink, utimes } from 'fs/promises';
import { spawn } from 'child_process';
import filePath from 'path';
import { StatOptions, Stats } from 'fs';

export const THREE_MONTHS = 90 * 86400 * 1000;

async function safeUnlink(path: string): Promise<boolean> {
  try {
    await unlink(path);
    return true;
  }
  catch {
    return false;
  }
}

async function safeStat(path: string, opts?: StatOptions & { bigint?: false, deleteIfEmpty?: boolean }): Promise<Stats> {
  try {
    let stats = await stat(path, opts);

    if (opts?.deleteIfEmpty && stats.size === 0) {
      await safeUnlink(path);
      stats = null;
    }

    return stats;
  }
  catch {
    return null;
  }
}

interface FileOpts extends ExtendedRequestOptions {
  unzipName?: string;
}

export async function getPossiblyCachedFile(file: string, url: string, name: string, extraOpts?: FileOpts): Promise<void> {
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
  let responseInfo: ResponseInfo;
  const opts: ExtendedRequestOptions = {
    cachePath: file,
    responseInfo: info => responseInfo = info,
    progress: () => {
      const now = processMillis();

      if (now > lastTick + 500) {
        tickShown = true;
        process.stdout.write('•');
        lastTick = now;
      }
    }
  };
  const stats = await safeStat(file, { deleteIfEmpty: true });
  let unzipName: string;

  if (extraOpts) {
    unzipName = extraOpts.unzipName;
    delete extraOpts.unzipName;
    Object.assign(opts, extraOpts);
  }

  let suffix = '';

  if (!stats)
    console.log(`Retrieving ${name}`);
  else
    suffix = '.tmp';

  try {
    await requestFile(url, opts, file + suffix);
    clearInterval(autoTick);

    if (tickShown)
      process.stdout.write('\n');

    if (suffix) {
      if (!responseInfo?.fromCache)
        await rename(file + suffix, file);
      else
        await safeUnlink(file + suffix);
    }

    const postStats = await safeStat(file, { deleteIfEmpty: true });

    if (stats) {
      if (postStats.mtimeMs > stats.mtimeMs)
        console.log(`Updating ${name}`);
      else
        console.log(`Using cached ${name}`);
    }

    if (unzipName) {
      const statZip = await safeStat(file);
      const statUnzip = await safeStat(unzipName, { deleteIfEmpty: true });

      if (!statUnzip || statUnzip.mtimeMs < statZip.mtimeMs - 1) {
        console.log(`Unzipping ${name}`);

        if (statUnzip)
          await unlink(unzipName);

        let zipProc = spawn('unzip', ['-Z1', file]);
        let originalZipName = '';

        await new Promise<void>(resolve => {
          zipProc.once('error', () => resolve());
          zipProc.stdout.on('data', data => originalZipName += data.toString());
          zipProc.stdout.once('end', () => resolve());
        });

        originalZipName = filePath.join('cache', originalZipName.trim());
        await unlink(originalZipName).catch(noop);

        zipProc = spawn('unzip', ['-D', '-d', 'cache', file]);

        await new Promise<void>(resolve => {
          zipProc.once('error', () => resolve());
          zipProc.stdout.once('end', () => resolve());
        });

        await rename(originalZipName, unzipName);
        await utimes(unzipName, statZip.mtime, statZip.mtime);
      }
      else
        console.log(`Using cached unzipped ${name}`);
    }
  }
  catch (err) {
    if (suffix && (await safeStat(file + suffix)))
      await unlink(file + suffix);

    console.error(`Failed to acquire ${name}. Will used cached copy.`);
    await readFile(file, 'utf-8');
  }
}
