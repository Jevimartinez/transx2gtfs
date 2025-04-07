import warnings
import pandas as pd


def get_mode(mode):
    """Parse mode from TransXChange value"""
    if mode in ['tram', 'trolleyBus']:
        return 0
    elif mode in ['underground', 'metro']:
        return 1
    elif mode == 'rail':
        return 2
    elif mode in ['bus', 'coach']:
        return 3
    elif mode == 'ferry':
        return 4
    return 3  # Default to bus if not found to prevent errors

def get_route_type(data):
    """Returns the route type according to GTFS reference"""
    # Try to get transport mode in a safe way
    service = data.TransXChange.Services.Service
    mode = getattr(service, 'Mode', None)

    mode = mode.cdata if mode else "Bus"
    return get_mode(mode)

def get_routes(gtfs_info, data):
    """Get routes from TransXchange elements"""
    use_cols = ['route_id', 'agency_id', 'route_short_name', 'route_long_name', 'route_type']

    routes = pd.DataFrame()

    # Check if Routes exists and has Route
    try:
        route_elements = data.TransXChange.Routes.Route
        if not isinstance(route_elements, list):
            route_elements = [route_elements]
    except AttributeError:
        warnings.warn("Couldn't find Route elements in TransXChange.Routes. Returning empty DataFrame.")
        return pd.DataFrame(columns=use_cols)

    # Check if gtfs_info has necessary columns
    required_cols = ['route_id', 'agency_id']
    if missing_cols := [
        col for col in required_cols if col not in gtfs_info.columns
    ]:
        warnings.warn(f"The following columns are missing in gtfs_info: {missing_cols}. Could not assign agency_id.")
        agency_id_default = "UNKNOWN_AGENCY"
    else:
        agency_id_default = None

    for r in route_elements:
        # Get route id
        route_id = r.get_attribute('id')

        # Get agency_id
        if agency_id_default is not None:
            agency_id = agency_id_default
        else:
            try:
                agency_id = gtfs_info.loc[gtfs_info['route_id'] == route_id, 'agency_id'].unique()[0]
            except IndexError:
                warnings.warn(f"Could not find agency_id for route_id {route_id}. Using default value.")
                agency_id = "UNKNOWN_AGENCY"

        # Get route long name
        route_long_name = r.Description.cdata

        # Get route private id
        route_private_id = r.PrivateCode.cdata

        # Get route short name (test '-_-' separator)
        route_short_name = route_private_id.split('-_-')[0]

        # Route Section reference (might be needed somewhere)
        route_section_id = r.RouteSectionRef.cdata

        # Get route_type
        route_type = get_route_type(data)

        # Generate row como DataFrame directamente
        route = pd.DataFrame([{
            'route_id': route_id,
            'agency_id': agency_id,
            'route_private_id': route_private_id,
            'route_long_name': route_long_name,
            'route_short_name': route_short_name,
            'route_type': route_type,
            'route_section_id': route_section_id
        }])

        # Add to container usando pd.concat
        routes = pd.concat([routes, route], ignore_index=True)

    # If no routes were generated, return empty DataFrame with expected columns
    if routes.empty:
        warnings.warn("No routes generated. Returning empty DataFrame.")
        return pd.DataFrame(columns=use_cols)

    # Ensure that route type is integer
    routes['route_type'] = routes['route_type'].astype(int)

    # Select only required columns
    routes = routes[use_cols].copy()
    return routes