import logging
import os
import re
from dataclasses import fields

import requests
from datetime import datetime
import jsonpickle
from requests.exceptions import ChunkedEncodingError
from rich.progress import Progress, TransferSpeedColumn
from rich.table import Table
from rich.tree import Tree
from urllib3.exceptions import IncompleteRead
import click
from rich.console import Console

import fetchtv_cli.helpers.upnp as upnp

try:
    from urlparse import urlparse
except ImportError:
    pass

from rich.traceback import install

install(show_locals=True)

SAVE_FILE = 'fetchtv_save_list.json'
FETCHTV_PORT = 49152
CONST_LOCK = '.lock'
MAX_FILENAME = 255
REQUEST_TIMEOUT = 5
MAX_OCTET = 4398046510080

console = Console(highlight=False, log_path=False)


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
        with open(path + os.path.sep + SAVE_FILE, 'a+') as read_file:
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
        with open(self.path + os.path.sep + SAVE_FILE, 'w') as write_file:
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
    progress = Progress(
        *Progress.get_default_columns(),
        TransferSpeedColumn(),
        console=console,
    )
    console.log(f'Writing: [{item.title}] to [{filename}]', markup=False)
    with requests.get(item.url, stream=True) as r:
        r.raise_for_status()
        total_length = int(r.headers.get('content-length'))
        if total_length == MAX_OCTET:
            msg = "Skipping item it's currently recording"
            print_warning(msg)
            json_result['warning'] = msg
            return False

        try:
            with open(filename + CONST_LOCK, 'xb') as f:
                with progress:
                    task = progress.add_task('Downloading', total=total_length)
                    progress.start_task(task)
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:  # filter out keep-alive new chunks
                            f.write(chunk)
                            progress.update(task, advance=len(chunk))

        except FileExistsError:
            msg = 'Already writing (lock file exists) skipping'
            print_warning(msg)
            json_result['warning'] = msg
            return False
        except ChunkedEncodingError as err:
            if err.args:
                try:
                    if isinstance(err.args[0].args[1], IncompleteRead):
                        msg = 'Final read was short; FetchTV sets the wrong Content-Length header. File should be fine'
                except IndexError:
                    msg = f'Chunked encoding error occurred. Content length was {total_length}. Error was: {err}'

            print_warning(msg)
            json_result['warning'] = msg
        except IOError as err:
            msg = f'Error writing file: {err}'
            print_error(msg)
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
    return not (
        folder
        and not next(
            (
                include_folder
                for include_folder in folder
                if recording.title.lower().find(include_folder.strip().lower()) != -1
            ),
            False,
        )
    )


def has_exclude_folder(recording, exclude):
    return exclude and next(
        (
            exclude_folder
            for exclude_folder in exclude
            if recording.title.lower().find(exclude_folder.strip().lower()) != -1
        ),
        False,
    )


def has_title_match(item, title):
    return not (
        title
        and not next(
            (include_title for include_title in title if item.title.lower().find(include_title.strip().lower()) != -1),
            False,
        )
    )


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
    console.print('Starting discovery')
    try:
        location_urls = upnp.ssdp_discovery(st='urn:schemas-upnp-org:device:MediaServer:1') if not ip else ['http://%s:%i/MediaServer.xml' % (ip, port)]
        locations = upnp.parse_locations(location_urls)
        # Find fetch
        result = [location for location in locations if location.manufacturerURL == 'http://www.fetch.com/']
        if len(result) == 0:
            print_error('Discovery failed: ERROR: Unable to locate Fetch UPNP service')
            return None
        console.print(f'Discovery successful: {result[0].url}')
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
        print_item('There is nothing new to record')
    return json_result


def print_item(param):
    console.print(f'{param}', markup=False)


def print_warning(param):
    console.print(f'[bold yellow]{param}')


def print_error(param, level=2):
    console.print(f'[bold red]{param}')


def print_heading(param):
    console.rule(title=param)


def create_item(item):
    item_type = 'episode' if re.match('^S\\d+ E\\d+', item.title) else 'movie'
    return {
        'id': item.id,
        'title': item.title,
        'type': item_type,
        'duration': item.duration,
        'size': item.size,
        'description': item.description,
    }


def print_recordings(recordings, output_json):
    if not output_json:
        print_heading('List recordings')
        if not recordings:
            print_warning('No recordings found!')

        tree = Tree('Recordings')
        for recording in recordings:
            if recording['items']:
                title = tree.add(f'[green]:file_folder: {recording["title"]}')
                for item in recording['items']:
                    title.add(f'{item.title} ({item.url})')
        console.print(tree)
    else:
        output = []
        for recording in recordings:
            items = []
            output.append({'id': recording['id'], 'title': recording['title'], 'items': items})
            for item in recording['items']:
                items.append(create_item(item))
        console.print_json(data=output)


@click.command()
@click.option('--info', is_flag=True, help='Attempts auto-discovery and returns the Fetch Servers details')
@click.option('--recordings', is_flag=True, help='List or save recordings')
@click.option('--shows', is_flag=True, help='List the names of shows with available recordings')
@click.option('--isrecording', is_flag=True, help='List any items that are currently recording')
@click.option('--ip', default=None, help='Specify the IP Address of the Fetch Server, if auto-discovery fails')
@click.option('--port', default=FETCHTV_PORT, help='Specify the port of the Fetch Server, if auto-discovery fails')
@click.option('--overwrite', is_flag=True, help='Will save and overwrite any existing files')
@click.option('--save', default=None, help='Save recordings to the specified path')
@click.option(
    '--folder', default=None, multiple=True, help='Only return recordings where the folder contains the specified text'
)
@click.option('--exclude', default=None, multiple=True, help="Don't download folders containing the specified text")
@click.option(
    '--title', default=None, multiple=True, help='Only return recordings where the item contains the specified text'
)
@click.option('--json', is_flag=True, help='Output show/recording/save results in JSON')
@click.option('--debug', is_flag=True, help='Enable debug mode')
def main(info, recordings, shows, isrecording, ip, port, overwrite, save, folder, exclude, title, json, debug):
    if debug:
        import http.client as http_client

        http_client.HTTPConnection.debuglevel = 1
        logging.basicConfig(level=logging.DEBUG)
        logging.getLogger().setLevel(logging.DEBUG)
        requests_log = logging.getLogger('requests.packages.urllib3')
        requests_log.setLevel(logging.DEBUG)
        requests_log.propagate = True
        logging.debug('Debug mode is enabled')

    print_heading(f'Started: {datetime.now():%Y-%m-%d %H:%M:%S}')
    with console.status('Discover Fetch UPnP location...'):
        fetch_server = discover_fetch(ip=ip, port=port)

    if not fetch_server:
        return

    if info:
        table = Table(title='Fetch TV box info', show_header=False)
        table.add_column('Field', justify='left', style='bold')
        table.add_column('Value', justify='left')

        # Loop over the fields of the Location dataclass
        for field in fields(fetch_server):
            value = getattr(fetch_server, field.name)
            table.add_row(field.name, str(value))

        console.print(table)

    if recordings or shows or isrecording:
        with console.status('Getting Fetch recordings...'):
            recordings = get_fetch_recordings(fetch_server, folder, exclude, title, shows, isrecording)
        if not save:
            print_recordings(recordings, json)
        else:
            # with console.status('Saving recordings'):
            print_heading('Saving recordings')
            json_result = save_recordings(recordings, save, overwrite)
            if json:
                console.print_json(data=json_result)
    print_heading(f'Done: {datetime.now():%Y-%m-%d %H:%M:%S}')


if __name__ == '__main__':
    main()
