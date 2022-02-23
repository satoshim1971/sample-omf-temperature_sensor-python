# NOTE: this script was designed using the v1.1
# version of the OMF specification, as outlined here:
# https://omf-docs.osisoft.com/documentation_v11/Whats_New.html
# *************************************************************************************

# ************************************************************************
# Import necessary packages
# ************************************************************************
import enum
import json
import requests
import time
import datetime
import gzip
import random
import traceback
import xml.etree.ElementTree as ET
from urllib.parse import urlparse


ERROR_STRING = 'Error'
TYPE_ID = 'Temperature.Float'
CONTAINER_ID = 'Sample.Script.SL6658.Temperature'

# List of possible endpoint types
# NOTE: OCS endpoint type is deprecated as OSIsoft Cloud Services has now been migrated to AVEVA Data Hub, use ADH type instead.
class EndpointTypes(enum.Enum):
    ADH = 'ADH'
    EDS = 'EDS'
    PI = 'PI'

# The version of the OMF messages
omf_version = '1.1'


def get_token(endpoint):
    '''Gets the token for the omfendpoint'''

    endpoint_type = endpoint["EndpointType"]
    # return an empty string if the endpoint is not an ADH type
    if endpoint_type != EndpointTypes.ADH:
        return ''

    if (('expiration' in endpoint) and (endpoint["expiration"] - time.time()) > 5 * 60):
        return endpoint["token"]

    # we can't short circuit it, so we must go retreive it.

    discovery_url = requests.get(
        endpoint["Resource"] + '/identity/.well-known/openid-configuration',
        headers={'Accept': 'application/json'},
        verify=endpoint["VerifySSL"])

    if discovery_url.status_code < 200 or discovery_url.status_code >= 300:
        discovery_url.close()
        raise Exception(f'Failed to get access token endpoint from discovery URL: {discovery_url.status_code}:{discovery_url.text}')

    token_endpoint = json.loads(discovery_url.content)["token_endpoint"]
    token_url = urlparse(token_endpoint)
    # Validate URL
    assert token_url.scheme == 'https'
    assert token_url.geturl().startswith(endpoint["Resource"])

    token_information = requests.post(
        token_url.geturl(),
        data={'client_id': endpoint["ClientId"],
              'client_secret': endpoint["ClientSecret"],
              'grant_type': 'client_credentials'},
        verify=endpoint["VerifySSL"])

    token = json.loads(token_information.content)

    if token is None:
        raise Exception('Failed to retrieve Token')

    __expiration = float(token["expires_in"]) + time.time()
    __token = token["access_token"]

    # cache the results
    endpoint["expiration"] = __expiration
    endpoint["token"] = __token

    return __token


def send_message_to_omf_endpoint(endpoint, message_type, message_omf_json, action='create'):
    '''Sends the request out to the preconfigured endpoint'''

    # Compress json omf payload, if specified
    compression = 'none'
    if endpoint["UseCompression"]:
        msg_body = gzip.compress(bytes(json.dumps(message_omf_json), 'utf-8'))
        compression = 'gzip'
    else:
        msg_body = json.dumps(message_omf_json)

    # Collect the message headers
    msg_headers = get_headers(endpoint, compression, message_type, action)

    # Send message to OMF endpoint
    endpoints_type = endpoint["EndpointType"]
    response = {}
    # If the endpoint is ADH
    if endpoints_type == EndpointTypes.ADH:
        response = requests.post(
            endpoint["OmfEndpoint"],
            headers=msg_headers,
            data=msg_body,
            verify=endpoint["VerifySSL"],
            timeout=endpoint["WebRequestTimeoutSeconds"]
        )
    # If the endpoint is EDS
    elif endpoints_type == EndpointTypes.EDS:
        response = requests.post(
            endpoint["OmfEndpoint"],
            headers=msg_headers,
            data=msg_body,
            timeout=endpoint["WebRequestTimeoutSeconds"]
        )
    # If the endpoint is PI
    elif endpoints_type == EndpointTypes.PI:
        response = requests.post(
            endpoint["OmfEndpoint"],
            headers=msg_headers,
            data=msg_body,
            verify=endpoint["VerifySSL"],
            timeout=endpoint["WebRequestTimeoutSeconds"],
            auth=(endpoint["Username"], endpoint["Password"])
        )

    # Check for 409, which indicates that a type with the specified ID and version already exists.
    if response.status_code == 409:
        return

    # response code in 200s if the request was successful!
    if response.status_code < 200 or response.status_code >= 300:
        print(msg_headers)
        response.close()
        print(
            f'Response from relay was bad. {message_type} message: {response.status_code} {response.text}.  Message holdings: {message_omf_json}')
        print()
        raise Exception(f'OMF message was unsuccessful, {message_type}. {response.status_code}:{response.text}')


def get_headers(endpoint, compression='', message_type='', action=''):
    '''Assemble headers for sending to the endpoint's OMF endpoint'''

    endpoint_type = endpoint["EndpointType"]

    msg_headers = {
        'messagetype': message_type,
        'action': action,
        'messageformat': 'JSON',
        'omfversion': omf_version
    }

    if(compression == 'gzip'):
        msg_headers["compression"] = 'gzip'

    # If the endpoint is ADH
    if endpoint_type == EndpointTypes.ADH:
        msg_headers["Authorization"] = f'Bearer {get_token(endpoint)}'
    # If the endpoint is PI
    elif endpoint_type == EndpointTypes.PI:
        msg_headers["x-requested-with"] = 'xmlhttprequest'

    # validate headers to prevent injection attacks
    validated_headers = {}

    for key in msg_headers:
        if key in {'Authorization', 'messagetype', 'action', 'messageformat', 'omfversion', 'x-requested-with', 'compression'}:
            validated_headers[key] = msg_headers[key]

    return validated_headers


def sanitize_headers(headers):
    validated_headers = {}

    for key in headers:
        if key in {'Authorization', 'messagetype', 'action', 'messageformat', 'omfversion', 'x-requested-with'}:
            validated_headers[key] = headers[key]

    return validated_headers


def one_time_send_creates(endpoint):
    action = 'create'
    one_time_send_type(endpoint, action)
    one_time_send_container(endpoint, action)
    one_time_send_data(endpoint, action)


def one_time_send_deletes(endpoint):
    print()
    print("Deleting sample data...")
    print()
    action = 'delete'
    try:
        one_time_send_data(endpoint, action)
    except Exception as ex:
        print()
        # Ignore errors in deletes to ensure we clean up as much as possible
        print(("Error in deletes: {error}".format(error=ex)))
        print()

    try:
        one_time_send_container(endpoint, action)
    except Exception as ex:
        print()
        # Ignore errors in deletes to ensure we clean up as much as possible
        print(("Error in deletes: {error}".format(error=ex)))
        print()

    try:
        one_time_send_type(endpoint, action)
    except Exception as ex:
        print()
        # Ignore errors in deletes to ensure we clean up as much as possible
        print(("Error in deletes: {error}".format(error=ex)))
        print()


def one_time_send_type(endpoint, action):
    # OMF Type messages
    send_message_to_omf_endpoint(endpoint, "type", [
        {
            "id": "RemoteAssets.RootType",
            "classification": "static",
            "type": "object",
            "description": "Root remote asset type",
            "properties": {
                "index": {
                    "type": "string",
                    "isindex": True
                },
                "name": {
                    "type": "string",
                    "isname": True
                }
            }
        },
        {
            "id": "RemoteAssets.FuelPumpType",
            "classification": "static",
            "type": "object",
            "description": "Remote pump asset type",
            "properties": {
                "index": {
                    "type": "string",
                    "isindex": True
                },
                "name": {
                    "type": "string",
                    "isname": True
                },
                "Desctiption": {
                    "type": "string",
                    "description": "Description of the asset"
                },
                "Location": {
                    "type": "string",
                    "description": "Location of the asset"
                }
            }
        },
        {
            "id": TYPE_ID,
            "name": "Temperature Float Type",
            "classification": "dynamic",
            "type": "object",
            "properties": {
                "Timestamp": {
                    "format": "date-time",
                    "type": "string",
                    "isindex": True
                },
                "Temperature": {
                    "type": "number",
                    "description": "Temperature readings",
                    "uom": "°F"
                }
            }
        }
    ], action)


def one_time_send_container(endpoint, action):
    # OMF Container message to create a container for our measurement
    send_message_to_omf_endpoint(endpoint, "container", [
        {
            "id": CONTAINER_ID,
            "name": "Temperature",
            "typeid": TYPE_ID,
            "description": "Container holds temperature measurements"
        }
    ], action)


def one_time_send_data(endpoint, action):
    # OMF Data message to create static elements and create links in AF
    send_message_to_omf_endpoint(endpoint, "data", [
        {
            "typeid": "RemoteAssets.RootType",
            "values": [
                {
                    "index": "RemoteAssets.Pumps.Root",
                    "name": "Remote Fuel Pumps"
                }
            ]
        },
        {
            "typeid": "RemoteAssets.FuelPumpType",
            "values": [
                {
                    "index": "RemoteAssets.Pump.SL6658",
                    "name": "SL6658 Pump",
                    "Desctiption": "Fuel pump asset",
                    "Location": "SLTC, San Leandro, California"
                }
            ]
        },
        {
            "typeid": "__Link",
            "values": [
                {
                    "source": {
                        "typeid": "RemoteAssets.RootType",
                        "index": "RemoteAssets.Pumps.Root"
                    },
                    "target": {
                        "typeid": "RemoteAssets.FuelPumpType",
                        "index": "RemoteAssets.Pump.SL6658"
                    }
                },
                {
                    "source": {
                        "typeid": "RemoteAssets.FuelPumpType",
                        "index": "RemoteAssets.Pump.SL6658"
                    },
                    "target": {
                        "containerid": CONTAINER_ID
                    }
                }
            ]
        }
    ], action)


def create_data_value(value):
    """Creates a JSON packet containing data value for the container"""
    return [
        {
            "containerid": CONTAINER_ID,
            "values": [
                {
                    "Timestamp": get_current_time(),
                    "Temperature": value
                }
            ]
        }
    ]


def get_random_value():
    """Returns random integer value in 200 - 500 range"""
    value = random.randrange(200, 500)
    return str(value)


def get_sensor_value(sensor_url):
    """Simple data collection logic"""
    try:
        response = requests.get(sensor_url)

        if (response.status_code == 200):
            decodedResponse = response.content.decode("utf-8")
            xmlRoot = ET.fromstring(decodedResponse)
            temperatureValue = xmlRoot.find('temperature').text
            print("Sensor value: ", temperatureValue)
            return temperatureValue
        else:
            return ERROR_STRING
    except Exception as ex:
        print(("Encountered Error: {error}".format(error=ex)))
        return ERROR_STRING


def get_current_time():
    """Returns the current time in UTC format"""
    return datetime.datetime.utcnow().isoformat() + 'Z'


def get_json_file(filename):
    ''' Get a json file by the path specified relative to the application's path'''

    # Try to open the configuration file
    try:
        with open(
            filename,
            'r',
        ) as f:
            loaded_json = json.load(f)
    except Exception as error:
        print(f'Error: {str(error)}')
        print(f'Could not open/read file: {filename}')
        exit()

    return loaded_json


def get_appsettings():
    ''' Return the appsettings.json as a json object, while also populating base_endpoint, omf_endpoint, and default values'''

    # Try to open the configuration file
    appsettings = get_json_file('appsettings.json')
    endpoints = appsettings["Endpoints"]

    # for each endpoint construct the check base and OMF endpoint and populate default values
    for endpoint in endpoints:
        if endpoint["EndpointType"] == 'OCS':
            print('OCS endpoint type is deprecated as OSIsoft Cloud Services has now been migrated to AVEVA Data Hub, using ADH type instead.')
            endpoint_type = EndpointTypes.ADH
        else:
            endpoint["EndpointType"] = EndpointTypes(endpoint["EndpointType"])
            endpoint_type = endpoint["EndpointType"]

        # If the endpoint is ADH
        if endpoint_type == EndpointTypes.ADH:
            base_endpoint = f'{endpoint["Resource"]}/api/{endpoint["ApiVersion"]}' + \
                f'/tenants/{endpoint["TenantId"]}/namespaces/{endpoint["NamespaceId"]}'

        # If the endpoint is EDS
        elif endpoint_type == EndpointTypes.EDS:
            base_endpoint = f'{endpoint["Resource"]}/api/{endpoint["ApiVersion"]}' + \
                f'/tenants/default/namespaces/default'

        # If the endpoint is PI
        elif endpoint_type == EndpointTypes.PI:
            base_endpoint = endpoint["Resource"]

        else:
            raise ValueError('Invalid endpoint type')

        omf_endpoint = f'{base_endpoint}/omf'

        # add the base_endpoint and omf_endpoint to the endpoint configuration
        endpoint["BaseEndpoint"] = base_endpoint
        endpoint["OmfEndpoint"] = omf_endpoint

        # check for optional/nullable parameters
        if 'VerifySSL' not in endpoint or endpoint["VerifySSL"] == None:
            endpoint["VerifySSL"] = True

        if 'UseCompression' not in endpoint or endpoint["UseCompression"] == None:
            endpoint["UseCompression"] = True

        if 'WebRequestTimeoutSeconds' not in endpoint or endpoint["WebRequestTimeoutSeconds"] == None:
            endpoint["WebRequestTimeoutSeconds"] = 30

    return appsettings


def main(test=False):
    global omfVersion
    try:
        print('------------------------------------------------------------------')
        print(' .d88888b.  888b     d888 8888888888        8888888b. Y88b   d88P ')
        print('d88P" "Y88b 8888b   d8888 888               888   Y88b Y88b d88P  ')
        print('888     888 88888b.d88888 888               888    888  Y88o88P   ')
        print('888     888 888Y88888P888 8888888           888   d88P   Y888P    ')
        print('888     888 888 Y888P 888 888               8888888P"     888     ')
        print('888     888 888  Y8P  888 888               888           888     ')
        print('Y88b. .d88P 888   "   888 888               888           888     ')
        print(' "Y88888P"  888       888 888      88888888 888           888     ')
        print('------------------------------------------------------------------')

        # Sensor configuration
        appsettings = get_appsettings()
        endpoints = appsettings.get('Endpoints')
        useRandom = appsettings.get('UseRandom')
        sensorUrl = appsettings.get('SensorUrl')

        # Scanning configuration
        iterationCount = (int)(
            appsettings.get('NumberOfIterations'))
        delayBetweenRequests = (int)(
            appsettings.get('DelayBetweenRequests'))

        for endpoint in endpoints:
            if not endpoint["Selected"]:
                continue
            
            one_time_send_creates(endpoint)

            count = 0
            time.sleep(1)
            while count == 0 or ((not test) and count < iterationCount):
                # Use get_sensor_value() method when HW sensor is available or get_random_value() method to
                # generate random value for demonstration purposes.
                if (useRandom):
                    measurement = get_random_value()
                else:
                    measurement = get_sensor_value(sensorUrl)

                if(measurement == ERROR_STRING):
                    print('Unable to get data from the sensor...')
                else:
                    value = int(measurement)/10
                    print("Sending value: ", value)
                    message = create_data_value(value)
                    send_message_to_omf_endpoint(endpoint, 'data', message)

                time.sleep(delayBetweenRequests)
                count = count + 1

            if (test):
                one_time_send_deletes(endpoint)

        print('Complete!')
        return True

    except Exception as ex:
        print()
        msg = 'Encountered Error: {error}'.format(error=ex)
        print(msg)
        print()
        traceback.print_exc()
        print()
        if (test):
            one_time_send_deletes()
        assert False, msg


if __name__ == "__main__":
    main()
