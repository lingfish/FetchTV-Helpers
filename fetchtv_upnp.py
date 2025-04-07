#!/usr/bin/python
import json
import os
import sys
import re
import requests
from datetime import datetime
import jsonpickle
from pprint import pprint
from clint.textui import progress
from requests.exceptions import ChunkedEncodingError
from urllib3.exceptions import IncompleteRead
import click

import helpers.upnp as upnp

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

SAVE_FILE = "fetchtv_save_list.json"
FETCHTV_PORT = 49152
CONST_LOCK = '.lock'
MAX_FILENAME = 255
REQUEST_TIMEOUT = 5
MAX_OCTET = 4398046510080


class SavedFiles:
    """
    FetchTV recorded items that have already been saved
    Serialised to and from JSON
    """

    @staticmethod
    def load(path):
        """
        Instantiate from JSON file, if it exists
        """
        with open(path + os.path.sep + SAVE_FILE, "a+") as read_file:
            read_file.seek(0)
            content = read_file.read()
            inst = jsonpickle.loads(content) if content else SavedFiles()
            inst.path = path
            return inst

    def __init__(self):
        self.__files = {}
        self.path = ''

    def add_file(self, item):
        self.__files[item.id] = item.title
        # Serialise after each success
        with open(self.path + os.path.sep + SAVE_FILE, "w") as write_file:
            write_file.write(jsonpickle.dumps(self))

    def contains(self, item):
        return item.id in self.__files.keys()


def create_valid_filename(filename):
    result = filename.strip()
    # Remove special characters
    for c in '<>:"/\\|?*':
        result = result.replace(c, '')
    # Remove whitespace
    for c in '\t ':
        result = result.replace(c, '_')
    return result[:MAX_FILENAME]


def download_file(item, filename, json_result):
    """
    Download the url contents to a file
    """
    print_item('Writing: [%s] to [%s]' % (item.title, filename))
    with requests.get(item.url, stream=True) as r:
        r.raise_for_status()
        total_length = int(r.headers.get('content-length'))
        if total_length == MAX_OCTET:
            msg = 'Skipping item it\'s currently recording'
            print_warning(msg, level=2)
            json_result['warning'] = msg
            return False

        try:
            with open(filename + CONST_LOCK, 'xb') as f:
                for chunk in progress.bar(r.iter_content(chunk_size=8192), expected_size=(total_length / 8192) + 1):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)

        except FileExistsError:
            msg = 'Already writing (lock file exists) skipping'
            print_warning(msg, level=2)
            json_result['warning'] = msg
            return False
        except ChunkedEncodingError as err:
            if err.args:
                try:
                    if isinstance(err.args[0].args[1], IncompleteRead):
                        msg = f'Final read was short; FetchTV sets the wrong Content-Length header. File should be fine'
                except IndexError:
                    msg = f'Chunked encoding error occurred. Content length was {total_length}. Error was: {err}'

            print_warning(msg, level=2)
            json_result['warning'] = msg
        except IOError as err:
            msg = f'Error writing file: {err}'
            print_error(msg, level=2)
            json_result['error'] = msg
            return False

        os.rename(filename + CONST_LOCK, filename)
        return True


def get_fetch_recordings(location, folder, exclude, title, shows, is_recording):
    """
    Return all FetchTV recordings, or only for a particular folder if specified
    """
    api_service = upnp.get_services(location)
    base_folders = upnp.find_directories(api_service)
    recording = [folder for folder in base_folders if folder.title == 'Recordings']
    if len(recording) == 0:
        return []
    recordings = upnp.find_directories(api_service, recording[0].id)
    return filter_recording_items(folder, exclude, title, shows, is_recording, recordings)


def has_include_folder(recording, folder):
    return not (folder and
                not next((include_folder for include_folder in folder
                          if recording.title.lower().find(include_folder.strip().lower()) != -1), False))


def has_exclude_folder(recording, exclude):
    return (exclude and
            next((exclude_folder for exclude_folder in exclude
                  if recording.title.lower().find(exclude_folder.strip().lower()) != -1), False))


def has_title_match(item, title):
    return not (title and
                not next((include_title for include_title in title
                          if item.title.lower().find(include_title.strip().lower()) != -1), False))


def currently_recording(item):
    with requests.get(item.url, stream=True) as r:
        r.raise_for_status()
        total_length = int(r.headers.get('content-length'))
        return total_length == MAX_OCTET


def filter_recording_items(folder, exclude, title, shows, is_recording, recordings):
    """
    Process the returned FetchTV recordings and filter the results as per the provided options.
    """
    results = []
    for recording in recordings:
        result = {'title': recording.title, 'id': recording.id, 'items': []}
        # Skip not matching folders
        if not has_include_folder(recording, folder) or has_exclude_folder(recording, exclude):
            continue

        # Process recorded items
        if not shows:  # Include items
            for item in recording.items:
                # Skip not matching titles
                if not has_title_match(item, title):
                    continue

                # Only include recording item if requested
                if not is_recording or currently_recording(item):
                    result['items'].append(item)

        results.append(result)
        if is_recording:
            # Only return folders with a recording item
            results = [result for result in results if len(result['items']) > 0]
    return results


def discover_fetch(ip=False, port=FETCHTV_PORT):
    print_heading('Starting Discovery')
    try:
        location_urls = upnp.discover_pnp_locations() if not ip else ['http://%s:%i/MediaServer.xml' % (ip, port)]
        locations = upnp.parse_locations(location_urls)
        # Find fetch
        result = [location for location in locations if location.manufacturerURL == 'http://www.fetch.com/']
        if len(result) == 0:
            print_heading('Discovery failed', 'ERROR: Unable to locate Fetch UPNP service')
            return None
        print_heading('Discovery successful', result[0].url)
    except upnp.UpnpError as err:
        print_error(err)
        return None

    return result[0]


def save_recordings(recordings, save_path, overwrite):
    """
    Save all recordings for the specified folder (if not already saved)
    """
    some_to_record = False
    path = save_path
    saved_files = SavedFiles.load(path)
    json_result = []
    for show in recordings:
        for item in show['items']:
            if overwrite or not saved_files.contains(item):
                some_to_record = True
                directory = path + os.path.sep + create_valid_filename(show['title'])
                if not os.path.exists(directory):
                    os.makedirs(directory)
                file_path = directory + os.path.sep + create_valid_filename(item.title) + '.mpeg'

                result = {'item': create_item(item), 'recorded': False}
                json_result.append(result)
                # Check if already writing
                lock_file = file_path + CONST_LOCK
                if os.path.exists(lock_file):
                    msg = 'Already writing (lock file exists) skipping: [%s]' % item.title
                    print_item(msg)
                    result['warning'] = msg
                    continue

                if download_file(item, file_path, result):
                    result['recorded'] = True
                    saved_files.add_file(item)
    if not some_to_record:
        print('\t -- There is nothing new to record')
    return json_result


def print_item(param, level=1):
    space = '\t' * level
    print(f'{space} -- {param}')


def print_warning(param, level=2):
    space = '\t' * level
    print(f'{space} -- [!] {param}')


def print_error(param, level=2):
    space = '\t' * level
    print(f'{space} -- [!] {param}')


def create_item(item):
    item_type = 'episode' if re.match('^S\\d+ E\\d+', item.title) else 'movie'
    return {
        'id': item.id,
        'title': item.title,
        'type': item_type,
        'duration': item.duration,
        'size': item.size,
        'description': item.description
    }


def print_recordings(recordings, output_json):
    if not output_json:
        print_heading('List Recordings')
        if not recordings:
            print_warning('No recordings found!', level=1)
        for recording in recordings:
            print_item(recording['title'])
            for item in recording['items']:
                print_item(f'{item.title} ({item.url})', level=2)
    else:
        output = []
        for recording in recordings:
            items = []
            output.append({'id': recording['id'], 'title': recording['title'], 'items': items})
            for item in recording['items']:
                items.append(create_item(item))
        output = json.dumps(output, indent=2, sort_keys=False)
        print(output)
        return output


def print_heading(param, value=''):
    print(f'[+] {param}: {value}')


@click.command()
# @click.option('--help', is_flag=True, help='Display this help')
@click.option('--info', is_flag=True, help='Attempts auto-discovery and returns the Fetch Servers details')
@click.option('--recordings', is_flag=True, help='List or save recordings')
@click.option('--shows', is_flag=True, help='List the names of shows with available recordings')
@click.option('--isrecording', is_flag=True, help='List any items that are currently recording')
@click.option('--ip', default=None, help='Specify the IP Address of the Fetch Server, if auto-discovery fails')
@click.option('--port', default=FETCHTV_PORT, help='Specify the port of the Fetch Server, if auto-discovery fails')
@click.option('--overwrite', is_flag=True, help='Will save and overwrite any existing files')
@click.option('--save', default=None, help='Save recordings to the specified path')
@click.option('--folder', default=None, multiple=True, help='Only return recordings where the folder contains the specified text')
@click.option('--exclude', default=None, multiple=True, help='Don\'t download folders containing the specified text')
@click.option('--title', default=None, multiple=True, help='Only return recordings where the item contains the specified text')
@click.option('--json', is_flag=True, help='Output show/recording/save results in JSON')
def main(info, recordings, shows, isrecording, ip, port, overwrite, save, folder, exclude, title, json):
    print_heading('Started', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print_heading('Discover Fetch UPnP location')
    fetch_server = discover_fetch(ip=ip, port=port)

    if not fetch_server:
        return

    if info:
        pprint(vars(fetch_server))

    if recordings or shows or isrecording:
        # folder_list = folder.split(',') if folder else []
        # exclude_list = exclude.split(',') if exclude else []
        # title_list = title.split(',') if title else []
        recordings = get_fetch_recordings(fetch_server, folder, exclude, title, shows, isrecording)
        if not save:
            print_recordings(recordings, json)
        else:
            print_heading('Saving Recordings')
            json_result = save_recordings(recordings, save, overwrite)
            if json:
                output = json.dumps(json_result, indent=2, sort_keys=False)
                print(output)
    print_heading('Done', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


if __name__ == "__main__":
    main()
