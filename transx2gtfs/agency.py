import pandas as pd
import warnings

def get_agency_url(operator_code):
    """Get url for operators"""
    operator_urls = {
        'OId_LUL': "https://tfl.gov.uk/maps/track/tube",
        'OId_DLR': "https://tfl.gov.uk/modes/dlr/",
        'OId_TRS': "https://www.thamesriverservices.co.uk/",
        'OId_CCR': "https://www.citycruises.com/",
        'OId_CV': "https://www.thamesclippers.com/",
        'OId_WFF': "https://tfl.gov.uk/modes/river/woolwich-ferry",
        'OId_TCL': "https://tfl.gov.uk/modes/trams/",
        'OId_EAL': "https://www.emiratesairline.co.uk/",
        #'OId_CRC': "https://www.crownrivercruise.co.uk/",
    }
    if operator_code in list(operator_urls.keys()):
        return operator_urls[operator_code]
    else:
        return "NA"

def get_agency(data):
    """Parse agency information from TransXchange elements"""
    # Container
    agency_data = pd.DataFrame()

    # Access Operators
    operators = data.TransXChange.Operators

    # Try to obtain operator in multiple ways
    if hasattr(operators, 'Operator'):
        operator = operators.Operator
    elif hasattr(operators, 'LicensedOperator'):
        operator = operators.LicensedOperator
    else:
        # Warning and default value in case of an error
        warnings.warn("Operator or LicensedOperator element not found in TransXChange.Operators. Using default values.")
        agency_id = "UNKNOWN_AGENCY"
        agency_name = "Unknown Operator"
        agency_url = "http://example.com"  # Generic URL
        return _extracted_from_get_agency_20(
            agency_id, agency_name, agency_url, agency_data
        )
    # Agency id
    agency_id = operator.get_attribute('id')
    if not agency_id:
        warnings.warn("Could not find 'id' attribute in the operator. Using default value.")
        agency_id = "UNKNOWN_AGENCY"

    # Agency name 
    # Try multiple operator field names
    if hasattr(operator, 'OperatorNameOnLicence'):
        agency_name = operator.OperatorNameOnLicence.cdata
    elif hasattr(operator, 'OperatorShortName'):
        agency_name = operator.OperatorShortName.cdata
    elif hasattr(operator, 'OperatorName'):
        agency_name = operator.OperatorName.cdata
    else:
        agency_name = "Unknown Operator"  # Fallback if there's no name nombre

    # Agency url
    agency_url = get_agency_url(agency_id)

    return _extracted_from_get_agency_20(
        agency_id, agency_name, agency_url, agency_data
    )

def _extracted_from_get_agency_20(agency_id, agency_name, agency_url, agency_data):
    agency_tz = "Europe/London"
    agency_lang = "en"

    # Create row with default values
    agency = pd.DataFrame([{
        'agency_id': agency_id,
        'agency_name': agency_name,
        'agency_url': agency_url,
        'agency_timezone': agency_tz,
        'agency_lang': agency_lang
    }])
    agency_data = pd.concat([agency_data, agency], ignore_index=True)
    return agency_data