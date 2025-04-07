# fetchtv-cli

[![PyPI - Version](https://img.shields.io/pypi/v/fetchtv-cli.svg)](https://pypi.org/project/fetchtv-cli)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/fetchtv-cli.svg)](https://pypi.org/project/fetchtv-cli)

[![blog](https://img.shields.io/badge/blog-Nerd%20stuff-blue)](https://blog.lucid.net.au/category/nerd-stuff/)
![X (formerly Twitter) Follow](https://img.shields.io/twitter/follow/lingfish)

A CLI tool to download recordings from FetchTV using UPnP.

This is a fork of the original project called [FetchTV-Helpers](https://github.com/jinxo13/FetchTV-Helpers).

## Introduction

This tool allows you to download recordings from Fetch TV boxes.

It can also be used to query recording information on the Fetch TV box.

## Installation

The recommended way to install `fetchtv-cli` is to use [pipx](https://pipx.pypa.io/stable/).

After getting `pipx` installed, simply run:

```console
pipx install fetchtv-cli
```

Please [don't use pip system-wide](https://docs.python.org/3.11/installing/index.html#installing-into-the-system-python-on-linux).

You can of course also install it using classic virtualenvs.

### Functions:
- Autodiscover FetchTV DLNA server
- View server information
- List all recordings, or matches for specified shows or titles
- Save only new recordings, or save everything that matches shows or titles
- Get responses as JSON. This includes additional item attributes, e.g. file size, duration, type (episode or movie), description

### Usage:
```
fetchtv_upnp.py <command> <options>

e.g.
--> Display Fetch Server details
fetchtv_upnp.py --info

--> List all available recorded shows (doesn't include episodes)
fetchtv_upnp.py --recordings --ip=192.168.1.10 --port=49152 --shows

--> List only recordings that haven't been saved
fetchtv_upnp.py --recordings --new --ip=192.168.1.10 --port=49152

--> Return responses as JSON
fetchtv_upnp.py --recordings --json --ip=192.168.1.10 --port=49152

--> List all available recorded items (all shows and episodes)
fetchtv_upnp.py --recordings --ip=192.168.1.10 --port=49152

--> Save any new recordings to C:\\Temp
fetchtv_upnp.py --recordings --ip=192.168.1.10 --port=49152 --save="C:\\temp"

--> Save any new recordings to C:\\Temp apart from 2 Broke Girls
fetchtv_upnp.py --recordings --ip=192.168.1.10 --port=49152 --save="C:\\temp" --exclude="2 Broke Girls"

--> Save any new episodes for the show 2 Broke Girls to C:\\Temp
fetchtv_upnp.py --recordings --ip=192.168.1.10 --port=49152 --folder="2 Broke Girls" --save="C:\\temp"

--> Save episode containing 'S4 E12' for the show 2 Broke Girls to C:\\Temp
fetchtv_upnp.py --recordings --ip=192.168.1.10 --port=49152 --overwrite --folder="2 Broke Girls" --title="S4 E12" --save="C:\\temp"

--> Save episode containing 'S4 E12' or 'S4 E13' for the show 2 Broke Girls to C:\\Temp
fetchtv_upnp.py --recordings --ip=192.168.1.10 --port=49152 --overwrite --folder="2 Broke Girls" --title="S4 E12, S4 E13" --save="C:\\temp"

--> List anything currently recording 
fetchtv_upnp.py --isrecording --ip=192.168.1.10 --port=49152
```

Commands:

| Command       | Description                                                                                                                                     |
|---------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| --help        | Display this help                                                                                                                               |
| --info        | Attempts auto-discovery and returns the Fetch Servers details                                                                                   |
| --recordings  | List or save recordings                                                                                                                         |
| --shows       | List the names of shows with available recordings                                                                                               |
| --isrecording | List any items that are currently recording. If no filtering is specified this will scan all items on the Fetch server so it can take some time |


Options:

--ip=<ip_address>             --> Specify the IP Address of the Fetch Server, if auto-discovery fails
--port=<port>                 --> Specify the port of the Fetch Server, if auto-discovery fails, normally 49152
--overwrite                   --> Will save and overwrite any existing files
--save=<path>                 --> Save recordings to the specified path
--folder="<text>[,<text>]"    --> Only return recordings where the folder contains the specified text
--exclude="<text>[,<text>]"   --> Don't download folders containing the specified text
--title="<text>[,<text>]"     --> Only return recordings where the item contains the specified text
--json                        --> Output show/recording/save results in JSON
