#!/usr/bin/env python3
from os import walk, path as ospath
from aiofiles.os import remove as aioremove, path as aiopath, listdir, rmdir, makedirs
from aioshutil import rmtree as aiormtree, move
from shutil import rmtree, disk_usage
from magic import Magic
from asyncio import create_subprocess_exec
from asyncio.subprocess import PIPE
from re import split as re_split, I, search as re_search
from subprocess import run as srun
from sys import exit as sexit

from .exceptions import NotSupportedExtractionArchive
from bot import bot_cache, aria2, LOGGER, DOWNLOAD_DIR, get_client, GLOBAL_EXTENSION_FILTER
from bot.helper.ext_utils.bot_utils import sync_to_async, cmd_exec

ARCH_EXT = [".tar.bz2", ".tar.gz", ".bz2", ".gz", ".tar.xz", ".tar", ".tbz2", ".tgz", ".lzma2",
            ".zip", ".7z", ".z", ".rar", ".iso", ".wim", ".cab", ".apm", ".arj", ".chm",
            ".cpio", ".cramfs", ".deb", ".dmg", ".fat", ".hfs", ".lzh", ".lzma", ".mbr",
            ".msi", ".mslz", ".nsis", ".ntfs", ".rpm", ".squashfs", ".udf", ".vhd", ".xar"]

FIRST_SPLIT_REGEX = r'(\.|_)part0*1\.rar$|(\.|_)7z\.0*1$|(\.|_)zip\.0*1$|^(?!.*(\.|_)part\d+\.rar$).*\.rar$'

SPLIT_REGEX = r'\.r\d+$|\.7z\.\d+$|\.z\d+$|\.zip\.\d+$'


def is_first_archive_split(file):
    return bool(re_search(FIRST_SPLIT_REGEX, file))


def is_archive(file):
    return file.endswith(tuple(ARCH_EXT))


def is_archive_split(file):
    return bool(re_search(SPLIT_REGEX, file))


async def clean_target(path):
    if await aiopath.exists(path):
        LOGGER.info(f"Cleaning Target: {path}")
        if await aiopath.isdir(path):
            try:
                await aiormtree(path)
            except Exception:
                pass
        elif await aiopath.isfile(path):
            try:
                await aioremove(path)
            except Exception:
                pass


async def clean_download(path):
    if await aiopath.exists(path):
        LOGGER.info(f"Cleaning Download: {path}")
        try:
            await aiormtree(path)
        except Exception:
            pass


async def start_cleanup():
    get_client().torrents_delete(torrent_hashes="all")
    try:
        await aiormtree(DOWNLOAD_DIR)
    except Exception:
        pass
    await makedirs(DOWNLOAD_DIR, exist_ok=True)


def clean_all():
    aria2.remove_all(True)
    get_client().torrents_delete(torrent_hashes="all")
    try:
        rmtree(DOWNLOAD_DIR)
    except Exception:
        pass


def exit_clean_up(signal, frame):
    try:
        LOGGER.info(
            "Please wait, while we clean up and stop the running downloads")
        clean_all()
        srun(['pkill', '-9', '-f', f'gunicorn|{bot_cache["pkgs"][-1]}'])
        sexit(0)
    except KeyboardInterrupt:
        LOGGER.warning("Force Exiting before the cleanup finishes!")
        sexit(1)


async def clean_unwanted(path):
    LOGGER.info(f"Cleaning unwanted files/folders: {path}")
    for dirpath, _, files in await sync_to_async(walk, path, topdown=False):
        for filee in files:
            if filee.endswith(".!qB") or filee.endswith('.parts') and filee.startswith('.'):
                await aioremove(ospath.join(dirpath, filee))
        if dirpath.endswith((".unwanted", "splited_files_mltb", "copied_mltb")):
            await aiormtree(dirpath)
    for dirpath, _, files in await sync_to_async(walk, path, topdown=False):
        if not await listdir(dirpath):
            await rmdir(dirpath)


async def get_path_size(path):
    if await aiopath.isfile(path):
        return await aiopath.getsize(path)
    total_size = 0
    for root, dirs, files in await sync_to_async(walk, path):
        for f in files:
            abs_path = ospath.join(root, f)
            total_size += await aiopath.getsize(abs_path)
    return total_size


async def count_files_and_folders(path):
    total_files = 0
    total_folders = 0
    for _, dirs, files in await sync_to_async(walk, path):
        total_files += len(files)
        for f in files:
            if f.endswith(tuple(GLOBAL_EXTENSION_FILTER)):
                total_files -= 1
        total_folders += len(dirs)
    return total_folders, total_files


def get_base_name(orig_path):
    extension = next(
        (ext for ext in ARCH_EXT if orig_path.lower().endswith(ext)), ''
    )
    if extension != '':
        return re_split(f'{extension}$', orig_path, maxsplit=1, flags=I)[0]
    else:
        raise NotSupportedExtractionArchive(
            'File format not supported for extraction')


def get_mime_type(file_path):
    mime = Magic(mime=True)
    mime_type = mime.from_file(file_path)
    mime_type = mime_type or "text/plain"
    return mime_type


def check_storage_threshold(size, threshold, arch=False, alloc=False):
    free = disk_usage(DOWNLOAD_DIR).free
    if not alloc:
        if (not arch and free - size < threshold or arch and free - (size * 2) < threshold):
            return False
    elif not arch:
        if free < threshold:
            return False
    elif free - size < threshold:
        return False
    return True


async def join_files(path):
    files = await listdir(path)
    results = []
    for file_ in files:
        if re_search(r"\.0+2$", file_) and await sync_to_async(get_mime_type, f'{path}/{file_}') == 'application/octet-stream':
            final_name = file_.rsplit('.', 1)[0]
            cmd = f'cat {path}/{final_name}.* > {path}/{final_name}'
            _, stderr, code = await cmd_exec(cmd, True)
            if code != 0:
                LOGGER.error(f'Failed to join {final_name}, stderr: {stderr}')
            else:
                results.append(final_name)
        else:
            LOGGER.warning('No Binary files to join!')
    if results:
        LOGGER.info('Join Completed!')
        for res in results:
            for file_ in files:
                if re_search(fr"{res}\.0[0-9]+$", file_):
                    await aioremove(f'{path}/{file_}')

async def edit_metadata(listener, base_dir: str, media_file: str, outfile: str, metadata: str = ''):
    file = media_file
    LOGGER.info(f"Starting metadata modification for file: {file}")
    temp_file = outfile
    full_file_path = media_file
    temp_file_path = outfile
    key = metadata
    cmd = [
        'ffprobe', '-hide_banner', '-loglevel', 'error', '-print_format', 'json', '-show_streams', full_file_path
    ]
    process = await create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
    stdout, stderr = await process.communicate()

    if process.returncode != 0:
        LOGGER.error(f"Error getting stream info: {stderr.decode().strip()}")
        return file

    try:
        streams = json.loads(stdout)['streams']
    except KeyError:
        LOGGER.error(f"No streams found in the ffprobe output: {stdout.decode().strip()}")
        return file

    cmd = [
        bot_cache['pkgs'][2], '-y', '-i', full_file_path, '-c', 'copy',
        '-metadata:s:v:0', f'title={key}',
        '-metadata', f'title={key}',
        '-metadata', 'copyright=',
        '-metadata', 'description=',
        '-metadata', 'license=',
        '-metadata', 'LICENSE=',
        '-metadata', 'author=',
        '-metadata', 'summary=',
        '-metadata', 'comment=',
        '-metadata', 'artist=',
        '-metadata', 'album=',
        '-metadata', 'genre=',
        '-metadata', 'date=',
        '-metadata', 'creation_time=',
        '-metadata', 'language=',
        '-metadata', 'publisher=',
        '-metadata', 'encoder=',
        '-metadata', 'SUMMARY=',
        '-metadata', 'AUTHOR=',
        '-metadata', 'WEBSITE=',
        '-metadata', 'COMMENT=',
        '-metadata', 'ENCODER=',
        '-metadata', 'FILENAME=',
        '-metadata', 'MIMETYPE=',
        '-metadata', 'PURL=',
        '-metadata', 'ALBUM='
    ]

    audio_index = 0
    subtitle_index = 0
    first_video = False

    for stream in streams:
        stream_index = stream['index']
        stream_type = stream['codec_type']
        if stream_type == 'video':
            if not first_video:
                cmd.extend(['-map', f'0:{stream_index}'])
                first_video = True
            cmd.extend([f'-metadata:s:v:{stream_index}', f'title={key}'])
        elif stream_type == 'audio':
            cmd.extend(['-map', f'0:{stream_index}', f'-metadata:s:a:{audio_index}', f'title={key}'])
            audio_index += 1
        elif stream_type == 'subtitle':
            codec_name = stream.get('codec_name', 'unknown')
            if codec_name in ['webvtt', 'unknown']:
                LOGGER.warning(f"Skipping unsupported subtitle metadata modification: {codec_name} for stream {stream_index}")
            else:
                cmd.extend(['-map', f'0:{stream_index}', f'-metadata:s:s:{subtitle_index}', f'title={key}'])
                subtitle_index += 1
        else:
            cmd.extend(['-map', f'0:{stream_index}'])
                    
    cmd.append(temp_file_path)
    process = await create_subprocess_exec(*cmd, stderr=PIPE, stdout=PIPE)
    stdout, stderr = await process.communicate()
    code = process.returncode    
    if code == 0:
        await clean_target(media_file)
        listener.seed = False
        await move(outfile, base_dir)
        return outfile
    else:
        LOGGER.error("Error in Metadata")
        await clean_target(outfile)
        return media_file
