import logging
import re
import socket
from dataclasses import dataclass, field, InitVar

import requests
import xml.etree.ElementTree as ElementTree

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

DISCOVERY_TIMEOUT = 3
REQUEST_TIMEOUT = 5
NO_NUMBER_DEFAULT = ''

logger = logging.getLogger(__name__)

class UpnpError(Exception):
    pass


@dataclass
class Location:
    url: str
    xml: InitVar[str]
    deviceType: str = field(init=False)
    friendlyName: str = field(init=False)
    manufacturer: str = field(init=False)
    manufacturerURL: str = field(init=False)
    modelDescription: str = field(init=False)
    modelName: str = field(init=False)
    modelNumber: str = field(init=False)

    def __post_init__(self, xml):
        BASE_PATH = './{urn:schemas-upnp-org:device-1-0}device/{urn:schemas-upnp-org:device-1-0}'
        self.deviceType = get_xml_text(xml, BASE_PATH + 'deviceType')
        self.friendlyName = get_xml_text(xml, BASE_PATH + 'friendlyName')
        self.manufacturer = get_xml_text(xml, BASE_PATH + 'manufacturer')
        self.manufacturerURL = get_xml_text(xml, BASE_PATH + 'manufacturerURL')
        self.modelDescription = get_xml_text(xml, BASE_PATH + 'modelDescription')
        self.modelName = get_xml_text(xml, BASE_PATH + 'modelName')
        self.modelNumber = get_xml_text(xml, BASE_PATH + 'modelNumber')


@dataclass
class Folder:
    xml: InitVar[str]
    title: str = field(init=False)
    id: str = field(init=False)
    parent_id: str = field(init=False)
    items: list = field(default_factory=list, init=False)

    def __post_init__(self, xml):
        self.title = xml.find('./{http://purl.org/dc/elements/1.1/}title').text
        self.id = get_xml_attr(xml, 'id', NO_NUMBER_DEFAULT)
        self.parent_id = get_xml_attr(xml, 'parentID', NO_NUMBER_DEFAULT)

    def add_items(self, items):
        self.items = [itm for itm in items]



@dataclass
class ProtocolInfo:
    protocol: str
    network: str
    content_format: str
    additional_info: dict = field(default_factory=dict)

    @staticmethod
    def parse(protocol_info: str):
        """
        Parse the protocolInfo string into its components and interpret DLNA-specific fields.
        """
        try:
            # Split the protocolInfo into main parts
            protocol, network, content_format, additional_info = protocol_info.split(":", 3)

            # Parse the additional info into a dictionary
            additional_info_dict = {}
            if additional_info:
                for item in additional_info.split(";"):
                    if "=" in item:
                        key, value = item.split("=", 1)
                        additional_info_dict[key] = value

            # Interpret DLNA-specific fields
            dlna_info = {
                "DLNA.ORG_PN": ProtocolInfo.decode_dlna_pn(additional_info_dict.get("DLNA.ORG_PN", None)),  # Profile Name
                "DLNA.ORG_OP": ProtocolInfo.parse_dlna_op(additional_info_dict.get("DLNA.ORG_OP", None)),  # Operations
                "DLNA.ORG_PS": ProtocolInfo.parse_dlna_ps(additional_info_dict.get("DLNA.ORG_PS", None)),  # Play Speed
                "DLNA.ORG_CI": ProtocolInfo.parse_dlna_ci(additional_info_dict.get("DLNA.ORG_CI", None)),  # Conversion Indicator
                "DLNA.ORG_FLAGS": ProtocolInfo.parse_dlna_flags(additional_info_dict.get("DLNA.ORG_FLAGS", None)),  # Flags
            }

            return ProtocolInfo(
                protocol=protocol,
                network=network,
                content_format=content_format,
                additional_info=dlna_info
            )
        except ValueError:
            raise ValueError(f"Invalid protocolInfo format: {protocol_info}")

    @staticmethod
    def decode_dlna_pn(pn_value):
        """
        Decode DLNA.ORG_PN field (Profile Name) into a human-readable format.
        """
        if pn_value is None:
            return None

        # Example mapping of DLNA profile names to descriptions
        dlna_profiles = {
            "AVC_TS_MP_HD_AAC": "MPEG-2 Transport Stream with H.264 video and AAC audio",
            "AVC_TS_MP_SD_MPEG1_L3": "MPEG-2 Transport Stream with H.264 video and MPEG-1 Layer 3 audio",
            "MPEG_PS_PAL": "MPEG Program Stream with PAL video",
            "MPEG_PS_NTSC": "MPEG Program Stream with NTSC video",
            "WMVHIGH_FULL": "Windows Media Video High Profile",
            # Add more profiles as needed
        }

        return dlna_profiles.get(pn_value, f"Unknown profile: {pn_value}")

    @staticmethod
    def parse_dlna_op(op_value):
        """
        Parse DLNA.ORG_OP field (operations).
        """
        if op_value is None:
            return None
        # Bit 0: Streaming, Bit 1: Interactive
        operations = []
        if int(op_value, 16) & 0x01:
            operations.append("Streaming")
        if int(op_value, 16) & 0x02:
            operations.append("Interactive")
        return operations

    @staticmethod
    def parse_dlna_ps(ps_value):
        """
        Parse DLNA.ORG_PS field (play speed).
        """
        return "Time-based seeking supported" if ps_value == "1" else "Not supported"

    @staticmethod
    def parse_dlna_ci(ci_value):
        """
        Parse DLNA.ORG_CI field (conversion indicator).
        """
        return "Transcoded" if ci_value == "1" else "Not transcoded"

    @staticmethod
    def parse_dlna_flags(flags_value):
        """
        Parse DLNA.ORG_FLAGS field (flags).
        """
        if flags_value is None:
            return None
        # Interpret the 32-bit flags according to DLNA specs
        flags = int(flags_value, 16)
        return {
            "SenderPaced": bool(flags & 0x80000000),
            "TimeBasedSeek": bool(flags & 0x40000000),
            "ByteBasedSeek": bool(flags & 0x20000000),
            "FlagPlayContainer": bool(flags & 0x10000000),
            "FlagS0Increasing": bool(flags & 0x08000000),
            "FlagSNIncreasing": bool(flags & 0x04000000),
            "RTSPPause": bool(flags & 0x02000000),
            "StreamingMode": bool(flags & 0x01000000),
            "InteractiveMode": bool(flags & 0x00800000),
            "BackgroundMode": bool(flags & 0x00400000),
            "ConnectionStall": bool(flags & 0x00200000),
            "DLNA_V15": bool(flags & 0x00100000),
        }

@dataclass
class Item:
    xml: InitVar[str]
    type: str = field(init=False)
    title: str = field(init=False)
    id: str = field(init=False)
    parent_id: str = field(init=False)
    description: str = field(init=False)
    url: str = field(init=False)
    size: int = field(init=False)
    duration: int = field(init=False)
    parent_name: str = field(init=False)
    recorded: str = field(init=False)
    protocol_info: ProtocolInfo = field(init=False)

    def __post_init__(self, xml):
        self.type = xml.find('./{urn:schemas-upnp-org:metadata-1-0/upnp/}class').text
        self.title = xml.find('./{http://purl.org/dc/elements/1.1/}title').text
        self.id = get_xml_attr(xml, 'id', NO_NUMBER_DEFAULT)
        self.parent_id = get_xml_attr(xml, 'parentID', NO_NUMBER_DEFAULT)
        self.description = xml.find('./{urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/}description')
        self.description = self.description.text if self.description is not None else ''
        self.recorded = xml.find('./{urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/}recordedStartDateTime').text
        res = xml.find('./{urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/}res')
        self.url = res.text
        self.size = int(get_xml_attr(res, 'size', NO_NUMBER_DEFAULT))
        self.duration = ts_to_seconds(get_xml_attr(res, 'duration', '0'))
        self.parent_name = get_xml_attr(res, 'parentTaskName')
        self.protocol_info = ProtocolInfo.parse(get_xml_attr(res, 'protocolInfo', NO_NUMBER_DEFAULT))


def ts_to_seconds(ts):
    """
    Convert timestamp in the form HH:MM:SS to seconds.
    e.g. 00:31:27 = 1887 seconds
    """
    try:
        return sum(float(unit) * 60**i for i, unit in enumerate(reversed(ts.split(':'))))
    except ValueError:
        raise UpnpError(f'Invalid timestamp format: {ts}')


def get_xml_attr(xml, name, default=''):
    """
    Return the value of an attribute if it exists, otherwise return the default value.
    """
    return xml.attrib.get(name, default)


def ssdp_discovery(st='ssdp:all', timeout=3):
    """
    Perform SSDP M-SEARCH discovery to find UPnP devices on the network.

    :param st: Search target (default is 'ssdp:all')
    :param timeout: Timeout for receiving responses (in seconds)
    :return: List of discovered device locations
    """
    ssdp_request = (
        f'M-SEARCH * HTTP/1.1\r\n'
        f'HOST: 239.255.255.250:1900\r\n'
        f'MAN: "ssdp:discover"\r\n'
        f'MX: 1\r\n'
        f'ST: {st}\r\n\r\n'
    )

    # Create a UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.settimeout(timeout)
    sock.sendto(ssdp_request.encode('utf-8'), ('239.255.255.250', 1900))

    responses = []
    try:
        while True:
            data, _ = sock.recvfrom(1024)
            responses.append(data.decode('utf-8'))
    except socket.timeout:
        pass
    finally:
        sock.close()

    # Extract locations from responses
    location_regex = re.compile(r'LOCATION:\s*(.+)', re.IGNORECASE)
    locations = [
        location_regex.search(response).group(1).strip() for response in responses if location_regex.search(response)
    ]
    return locations


def get_xml_text(xml, xml_name, default=''):
    """
    Return the text value if it exists, if not return the default value
    """
    try:
        return xml.find(xml_name).text
    except AttributeError:
        return default


def parse_locations(locations):
    """
    Loads the XML at each location and prints out the API along with some other
    interesting data.

    @param locations a collection of URLs
    @return igd_ctr (the control address) and igd_service (the service type)
    """
    result = []
    if len(locations) > 0:
        for location in locations:
            try:
                resp = requests.get(location, timeout=REQUEST_TIMEOUT)
                resp.raise_for_status()
                try:
                    xml_root = ElementTree.fromstring(resp.text)
                except ElementTree.ParseError as err:
                    raise UpnpError(f'XML Parsing failed for location {location}, Error: {err.msg}')

                loc = Location(location, xml_root)
                result.append(loc)

            except requests.exceptions.ConnectionError:
                # raise UpnpError(f'Connection Error, could not load {location}, Error: {err}')
                continue
            except requests.exceptions.ReadTimeout:
                raise UpnpError(f'Timeout reading from {location}')
    return result


def get_services(location):
    parsed = urlparse(location.url)
    resp = requests.get(location.url, timeout=REQUEST_TIMEOUT)
    try:
        xml_root = ElementTree.fromstring(resp.text)
    except Exception as err:
        raise UpnpError(f'XML parsing failed for location: {location}, Error: {err.msg}')

    result = {}

    services = xml_root.findall('.//*{urn:schemas-upnp-org:device-1-0}serviceList/')
    for service in services:
        # Add a lead in '/' if it doesn't exist
        scp = service.find('./{urn:schemas-upnp-org:device-1-0}SCPDURL').text
        if scp[0] != '/':
            scp = '/' + scp
        service_url = parsed.scheme + '://' + parsed.netloc + scp

        # read in the SCP XML
        resp = requests.get(service_url, timeout=REQUEST_TIMEOUT)
        service_xml = ElementTree.fromstring(resp.text)

        actions = service_xml.findall('.//*{urn:schemas-upnp-org:service-1-0}action')
        for action in actions:
            if action.find('./{urn:schemas-upnp-org:service-1-0}name').text == 'Browse':
                result['service_url'] = service_url
                result['cd_ctr'] = (
                    parsed.scheme
                    + '://'
                    + parsed.netloc
                    + service.find('./{urn:schemas-upnp-org:device-1-0}controlURL').text
                )
                result['cd_service'] = service.find('./{urn:schemas-upnp-org:device-1-0}serviceType').text
                break
    return result


def find_directories(api_service, object_id='0'):
    """
    Send a 'Browse' request for the top level directory. We will print out the
    top level containers that we observer. I've limited the count to 10.

    @param p_url the url to send the SOAPAction to
    @param p_service the service in charge of this control URI
    """
    p_url = api_service['cd_ctr']
    p_service = api_service['cd_service']
    result = []
    payload = f"""
            <?xml version="1.0" encoding="utf-8" standalone="yes"?>
            <s:Envelope s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/" xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
            <s:Body>
            <u:Browse xmlns:u="{p_service}">
            <ObjectID>{object_id}</ObjectID>
            <BrowseFlag>BrowseDirectChildren</BrowseFlag>
            <Filter>*</Filter>
            <StartingIndex>0</StartingIndex>
            <SortCriteria></SortCriteria>
            </u:Browse>
            </s:Body>
            </s:Envelope>
            """

    soap_action_header = {'Soapaction': f'"{p_service}#Browse"', 'Content-type': 'text/xml;charset="utf-8"'}

    resp = requests.post(p_url, data=payload, headers=soap_action_header)
    if resp.status_code != 200:
        raise UpnpError(f'Request failed with status: {resp.status_code}')

    xml_root = ElementTree.fromstring(resp.text)
    containers = xml_root.find('.//*Result').text
    if not containers:
        return result

    xml_root = ElementTree.fromstring(containers)
    containers = xml_root.findall('./{urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/}container')
    for container in containers:
        if container.find('./{urn:schemas-upnp-org:metadata-1-0/upnp/}class').text.find('object.container') > -1:
            folder = Folder(container)
            result.append(folder)
            folder.add_items(find_items(p_url, p_service, container.attrib['id']))
    return result


def find_items(p_url, p_service, object_id):
    result = []
    payload = f"""
            <?xml version="1.0" encoding="utf-8" standalone="yes"?>
            <s:Envelope s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/" xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
            <s:Body>
            <u:Browse xmlns:u="{p_service}">
            <ObjectID>{object_id}</ObjectID>
            <BrowseFlag>BrowseDirectChildren</BrowseFlag>
            <Filter>*</Filter>
            <StartingIndex>0</StartingIndex>
            <SortCriteria></SortCriteria>
            </u:Browse>
            </s:Body>
            </s:Envelope>
            """
    soap_action_header = {'Soapaction': f'"{p_service}#Browse"', 'Content-type': 'text/xml;charset="utf-8"'}

    resp = requests.post(p_url, data=payload, headers=soap_action_header)
    if resp.status_code != 200:
        raise UpnpError(f'Request failed with status: {resp.status_code}')

    xml_root = ElementTree.fromstring(resp.text)
    containers = xml_root.find('.//*Result').text
    if not containers:
        return result

    xml_root = ElementTree.fromstring(containers)
    items = xml_root.findall('./{urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/}item')
    for item in items:
        itm = Item(item)
        result.append(itm)
        logger.debug(item)
    return result
