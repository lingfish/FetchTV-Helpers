import json
import os
import pytest
from click.testing import CliRunner

from src.fetchtv_cli import fetchtv_cli as fetchtv
import tempfile
from unittest.mock import Mock, patch, mock_open

OPTION_IP = '--ip'
OPTION_PORT = '--port'
OPTION_OVERWRITE = '--overwrite'
OPTION_FOLDER = '--folder'
OPTION_TITLE = '--title'
OPTION_EXCLUDE = '--exclude'
OPTION_SAVE = '--save'
OPTION_JSON = '--json'

CMD_RECORDINGS = '--recordings'
CMD_IS_RECORDING = '--isrecording'
CMD_INFO = '--info'
CMD_SHOWS = '--shows'
CMD_HELP = '--help'

URL_DUMMY = 'http://dummy'
URL_NO_RECORDINGS = 'http://no_recordings'

SHOW_ONE = '2 Broke Girls'
SHOW_ONE_EP_ONE = 'S4 E12'
SHOW_ONE_EP_TWO = 'S4 E13'

SHOW_TWO = 'Lego Masters'


def get_file(filename):
    with open(filename, mode='r') as file:
        return file.read()


def mock_get(p_url, timeout=0, stream=False):
    result = Mock()
    result.__enter__ = Mock(return_value=result)
    result.__exit__ = Mock()
    result.iter_content = Mock(return_value='0')
    result.status_code = 200
    # Simulate a recording item
    if p_url == 'http://192.168.1.147:49152/web/903106340':
        result.headers = {'content-length': fetchtv.MAX_OCTET}
    else:
        result.headers = {'content-length': 5}

    response_dir = os.path.dirname(__file__) + os.path.sep + 'responses' + os.path.sep
    if p_url.endswith('cds.xml'):
        result.text = get_file(response_dir + 'fetch_cds.xml')
    else:
        result.text = get_file(response_dir + 'fetch_info.xml')
    return result


def mock_get_recording(p_url, timeout=0, stream=False):
    result = Mock()
    result.__enter__ = Mock(return_value=result)
    result.__exit__ = Mock()
    result.iter_content = Mock(return_value='0')
    result.status_code = 200
    result.headers = {'content-length': fetchtv.MAX_OCTET}
    response_dir = os.path.dirname(__file__) + os.path.sep + 'responses' + os.path.sep
    if p_url.endswith('cds.xml'):
        result.text = get_file(response_dir + 'fetch_cds.xml')
    else:
        result.text = get_file(response_dir + 'fetch_info.xml')
    return result


def mock_post(p_url, data, headers):
    result = Mock()
    result.__enter__ = Mock()
    result.__exit__ = Mock()
    result.status_code = 200

    response_dir = os.path.dirname(__file__) + os.path.sep + 'responses' + os.path.sep
    if data.find('<ObjectID>61</ObjectID>') != -1:
        result.text = get_file(response_dir + 'fetch_recording_items.xml')
    elif data.find('<ObjectID>0</ObjectID>') != -1:
        if p_url.startswith(URL_NO_RECORDINGS):
            result.text = get_file(response_dir + 'fetch_no_recordings.xml')
        else:
            result.text = get_file(response_dir + 'fetch_base_folders.xml')
    else:
        result.text = get_file(response_dir + 'fetch_recording_folders.xml')
    return result


@pytest.mark.skip(reason='Not needed anymore due to using Click')
@patch('requests.get', mock_get)
@patch('requests.post', mock_post)
class TestOptions:

    def test_command_order(self):
        runner = CliRunner()
        result = runner.invoke(fetchtv.main, [CMD_INFO, CMD_RECORDINGS, CMD_SHOWS, CMD_HELP])
        assert 'Usage:' in result.output
        assert result.exit_code == 0

        result = runner.invoke(fetchtv.main, [CMD_SHOWS, CMD_RECORDINGS, CMD_INFO])
        assert 'Discover Fetch UPnP location' in result.output
        assert result.exit_code == 0

        result = runner.invoke(fetchtv.main, [CMD_SHOWS, CMD_RECORDINGS])
        assert 'Discover Fetch UPnP location' in result.output
        assert result.exit_code == 0

    def test_command_values(self):
        runner = CliRunner()
        result = runner.invoke(fetchtv.main, [CMD_HELP])
        assert 'Usage:' in result.output
        assert result.exit_code == 0

    def test_option_multi_value(self):
        runner = CliRunner()
        for option in [OPTION_FOLDER, OPTION_TITLE, OPTION_EXCLUDE]:
            result = runner.invoke(fetchtv.main, [f'{option}="wibble"'])
            assert result.exit_code == 0

            result = runner.invoke(fetchtv.main, [f'{option}="wibble, wobble, rabble"'])
            assert result.exit_code == 0

    def test_option_strip_quotes(self):
        runner = CliRunner()
        result = runner.invoke(fetchtv.main, [f'{CMD_INFO}="fred"'])
        assert result.exit_code == 0

        result = runner.invoke(fetchtv.main, [f'{CMD_INFO}=\'fred\''])
        assert result.exit_code == 0

    def test_option_strip_save(self):
        runner = CliRunner()
        result = runner.invoke(fetchtv.main, [f'{OPTION_SAVE}=fred' + os.path.sep])
        assert result.exit_code == 0

    def test_option_single_value(self):
        runner = CliRunner()
        for option in [OPTION_SAVE, OPTION_IP, OPTION_PORT]:
            result = runner.invoke(fetchtv.main, [f'{option}="wibble"'])
            assert result.exit_code == 0

            result = runner.invoke(fetchtv.main, [f'{option}="wibble, wobble, rabble"'])
            assert result.exit_code == 0


@patch('requests.get', mock_get)
@patch('requests.post', mock_post)
class TestGetFetchRecordings:

    def test_get_shows(self):
        fetch_server = Mock()
        fetch_server.url = URL_DUMMY
        results = fetchtv.get_fetch_recordings(fetch_server, [], [], [], True, False)
        fetchtv.print_recordings(results, False)
        assert len(results) == 8

    def test_get_shows_json(self):
        fetch_server = Mock()
        fetch_server.url = URL_DUMMY
        results = fetchtv.get_fetch_recordings(fetch_server, [], [], [], True, False)
        output = fetchtv.print_recordings(results, True)
        output = json.loads(output)
        assert len(output) == 8

    def test_no_recordings_folder(self):
        fetch_server = Mock()
        fetch_server.url = URL_NO_RECORDINGS
        results = fetchtv.get_fetch_recordings(fetch_server, [], [], [], False, False)
        fetchtv.print_recordings(results, False)
        assert len(results) == 0

    def test_get_all_recordings(self):
        fetch_server = Mock()
        fetch_server.url = URL_DUMMY
        results = fetchtv.get_fetch_recordings(fetch_server, [], [], [], False, False)
        fetchtv.print_recordings(results, False)
        assert len(results) == 8
        assert len(results[4]['items']) == 134

    def test_get_all_recordings_json(self):
        fetch_server = Mock()
        fetch_server.url = URL_DUMMY
        results = fetchtv.get_fetch_recordings(fetch_server, [], [], [], False, False)
        output = fetchtv.print_recordings(results, True)
        output = json.loads(output)
        assert len(output) == 8
        assert len(output[4]['items']) == 134

    def test_get_recordings_items_json(self):
        fetch_server = Mock()
        fetch_server.url = URL_DUMMY
        results = fetchtv.get_fetch_recordings(fetch_server, [], [], [], False, True)
        output = fetchtv.print_recordings(results, True)
        output = json.loads(output)
        assert len(output) == 1
        assert len(output[0]['items']) == 1

    def test_exclude_one_show(self):
        fetch_server = Mock()
        fetch_server.url = URL_DUMMY
        results = fetchtv.get_fetch_recordings(fetch_server, [], [SHOW_ONE], [], False, False)
        fetchtv.print_recordings(results, False)
        assert len(results) == 7

    def test_exclude_two_shows(self):
        fetch_server = Mock()
        fetch_server.url = URL_DUMMY
        results = fetchtv.get_fetch_recordings(fetch_server, [], [SHOW_ONE, SHOW_TWO], [], False, False)
        fetchtv.print_recordings(results, False)
        assert len(results) == 5  # Test data has LEGO Masters and Lego Masters - both are matched

    def test_get_one_show_recording(self):
        fetch_server = Mock()
        fetch_server.url = URL_DUMMY
        results = fetchtv.get_fetch_recordings(fetch_server, [SHOW_ONE], [], [], False, False)
        fetchtv.print_recordings(results, False)
        assert len(results) == 1
        assert len(results[0]['items']) == 134

    def test_get_two_show_recording(self):
        fetch_server = Mock()
        fetch_server.url = URL_DUMMY
        results = fetchtv.get_fetch_recordings(fetch_server, [SHOW_ONE, SHOW_TWO], [], [], False, False)
        fetchtv.print_recordings(results, False)
        assert len(results) == 3  # Test data returns LEGO Masters and Lego Masters....

    def test_get_one_recording_item(self):
        fetch_server = Mock()
        fetch_server.url = URL_DUMMY
        results = fetchtv.get_fetch_recordings(fetch_server, [SHOW_ONE], [], [SHOW_ONE_EP_ONE], False, False)
        fetchtv.print_recordings(results, False)
        assert len(results) == 1
        assert len(results[0]['items']) == 1

    def test_get_two_recording_item(self):
        fetch_server = Mock()
        fetch_server.url = URL_DUMMY
        results = fetchtv.get_fetch_recordings(fetch_server, [SHOW_ONE], [], [SHOW_ONE_EP_ONE, SHOW_ONE_EP_TWO], False, False)
        fetchtv.print_recordings(results, False)
        assert len(results) == 1
        assert len(results[0]['items']) == 2

@patch('requests.get', mock_get)
@patch('requests.post', mock_post)
class TestSaveRecordings:

    def test_already_saving_recording(self):
        fetch_server = Mock()
        fetch_server.url = URL_DUMMY
        temp_dir = tempfile.gettempdir()
        saved_files = fetchtv.SavedFiles.load(temp_dir)
        recordings = fetchtv.get_fetch_recordings(fetch_server, [SHOW_ONE], [], [SHOW_ONE_EP_ONE], False, False)
        show_folder = fetchtv.create_valid_filename(recordings[0]['title'])
        filename = fetchtv.create_valid_filename(recordings[0]['items'][0].title)
        lock_file = f'{temp_dir}{os.path.sep}{show_folder}{os.path.sep}{filename}.mpeg.lock'

        try:
            os.mkdir(temp_dir + os.path.sep + show_folder)
            with open(lock_file, 'x') as f:
                f.write('.')
            json_result = fetchtv.save_recordings(recordings, temp_dir, False)
            assert json_result[0]['warning'].startswith('Already writing')
            assert not json_result[0]['recorded']
        finally:
            os.remove(lock_file)
            os.rmdir(temp_dir + os.path.sep + show_folder)


@patch('requests.get', mock_get)
class TestDownloadFile:

    def test_save_item(self, tmp_path):
        # Test download works when item is not recording

        temp_file = tmp_path / "test.txt"

        mock_file = mock_open(read_data='xxx')
        mock_location = Mock()
        mock_location.url = URL_DUMMY
        with patch('requests.get', mock_get):
            with patch('fetchtv_upnp.open', mock_file):
                with patch('fetchtv_upnp.os.rename', Mock()):
                    self
