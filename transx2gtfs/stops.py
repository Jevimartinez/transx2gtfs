import os
import pandas as pd
import pyproj
import warnings
import io
import urllib.request
import tempfile

def _update_naptan_data(url="https://beta-naptan.dft.gov.uk/Download/National/csv",
                       filepath=None):
    """
    Downloads the NaPTAN data as a CSV file.
    """
    if filepath is None:
        temp_dir = tempfile.gettempdir()
        target_dir = os.path.join(temp_dir, 'transx2gtfs')
        target_file = os.path.join(target_dir, "NaPTAN_data.csv")

        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        if os.path.exists(target_file):
            print("Removing old stop data")
            try:
                os.remove(target_file)
            except PermissionError:
                print(f"Couldn't delete {target_file}.")
                raise

    else:
        target_file = filepath

    # Download CSV from the URL
    filepath, msg = urllib.request.urlretrieve(url, target_file)
    print(f"Downloaded/updated NaPTAN stop dataset to:\n'{filepath}'")

def read_naptan_stops(naptan_fp=None):
    """
    Reads NaPTAN stops from temp. Assumes the file already exists.
    """
    if naptan_fp is None:
        naptan_fp = os.path.join(tempfile.gettempdir(), 'transx2gtfs', 'NaPTAN_data.csv')

    if not os.path.exists(naptan_fp):
        raise FileNotFoundError(
            f"The NaPTAN file is not in '{naptan_fp}'. "
            "Make sure to download it before processing the data."
        )

    # Read the CSV file
    stops = pd.read_csv(naptan_fp, encoding='latin1', low_memory=False)

    # Rename required columns into GTFS format
    stops = stops.rename(columns={
        'ATCOCode': 'stop_id',
        'Longitude': 'stop_lon',
        'Latitude': 'stop_lat',
        'CommonName': 'stop_name',
    })

    # Keep only required columns
    required_cols = ['stop_id', 'stop_lon', 'stop_lat', 'stop_name']
    for col in required_cols:
        if col not in stops.columns:
            raise ValueError(
                f"Required column {col} could not be found from stops DataFrame."
            )
    stops = stops[required_cols].copy()
    return stops

def _get_tfl_style_stops(data):
    """
    Parse stops in TfL-style TransXChange format.
    """
    wgs84 = pyproj.Proj("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs")
    osgb36 = pyproj.Proj(
        "+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.999601 +x_0=400000 +y_0=-100000 +ellps=airy +towgs84=446.448,-125.157,542.060,0.1502,0.2470,0.8421,-20.4894 +units=m +no_defs")

    _stop_id_col = 'stop_id'
    stop_data = pd.DataFrame()
    naptan_stops = read_naptan_stops()

    for p in data.TransXChange.StopPoints.StopPoint:
        stop_name = p.Descriptor.CommonName.cdata
        stop_id = p.AtcoCode.cdata
        stop = naptan_stops.loc[naptan_stops[_stop_id_col] == stop_id]

        if len(stop) == 0:
            try:
                x = float(p.Place.Location.Easting.cdata)
                y = float(p.Place.Location.Northing.cdata)
                detected_epsg = 7405 if x > 180 else 4326

                if detected_epsg == 7405:
                    x, y = pyproj.transform(osgb36, wgs84, x, y)

                stop = pd.DataFrame([{
                    'stop_id': stop_id,
                    'stop_code': None,
                    'stop_name': stop_name,
                    'stop_lat': y,
                    'stop_lon': x,
                    'stop_url': None
                }])
            except Exception:
                warnings.warn(f"Did not find a NaPTAN stop for '{stop_id}'", UserWarning, stacklevel=2)
                continue
        elif len(stop) > 1:
            raise ValueError("Had more than 1 stop with identical stop reference.")
        
        stop_data = pd.concat([stop_data, stop], ignore_index=True)

    return stop_data

def _get_txc_21_style_stops(data):
    """
    Parse stops in TransXChange 2.1 style format.
    """
    _stop_id_col = 'stop_id'
    stop_data = pd.DataFrame()
    naptan_stops = read_naptan_stops()

    for p in data.TransXChange.StopPoints.AnnotatedStopPointRef:
        stop_id = p.StopPointRef.cdata
        stop = naptan_stops.loc[naptan_stops[_stop_id_col] == stop_id]

        if len(stop) == 0:
            warnings.warn(f"Did not find a NaPTAN stop for '{stop_id}'", UserWarning, stacklevel=2)
            continue
        elif len(stop) > 1:
            raise ValueError("Had more than 1 stop with identical stop reference.")

        stop_data = pd.concat([stop_data, stop], ignore_index=True)

    return stop_data

def get_stops(data):
    """Parse stop data from TransXchange elements"""
    if 'StopPoint' in dir(data.TransXChange.StopPoints):
        stop_data = _get_tfl_style_stops(data)
    elif 'AnnotatedStopPointRef' in dir(data.TransXChange.StopPoints):
        stop_data = _get_txc_21_style_stops(data)
    else:
        raise ValueError(
            "Did not find tag for Stop data in TransXchange xml. "
            "Could not parse Stop information from the TransXchange."
        )

    return None if len(stop_data) == 0 else stop_data