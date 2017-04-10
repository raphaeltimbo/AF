import clr
import pandas as pd
clr.AddReference('OSIsoft.AFSDK')
clr.AddReference('System.Net')
import OSIsoft.AF as AF
from System.Net import NetworkCredential


__all__ = [
    'AF',
    'NetworkCredential',
    'get_server',
    'get_tag',
    'interpolated_values',
    'search_tag_mask',
    'sample_data'
]


def get_server(server_name):
    """Connect to server"""
    PI_server = AF.PI.PIServers()[server_name]
    return PI_server


def get_tag(server, tag_name):
    """Get a tag.
    
    Parameters
    ----------
    server : PI_Server
    
    tag_name : str
    
    """
    return AF.PI.PIPoint.FindPIPoint(server, tag_name)


def interpolated_values(tag, time_range, time_span):
    """Return an object with interpolated values
    
    Parameters
    ----------
    tag : TAG object

    time_range : tuple
        Tuple with start time and end time as str.
    time_span : str
        Time span (e.g.: '1s', '1d'...)
        
    Returns
    -------
    interpolated_values
    
    """
    time_range = AF.Time.AFTimeRange(*time_range)
    time_span = AF.Time.AFTimeSpan.Parse(time_span)

    return tag.InterpolatedValues(time_range, time_span, '', '')


def search_tag_mask(server, tag_mask):
    """Search by tag mask.
    
    Parameters
    ----------
    tag_mask : str
        Tag mask (e.g.: *FI*290.033*)

    Returns
    -------
    tags list: list
        List with tags (as str) that match the search.
    """
    tags = AF.PI.PIPoint.FindPIPoints(server, tag_mask)

    return [tag.Name for tag in tags]


def sample_data(server, tags, time_range, time_span):
    """Get sample data.
    
    Parameters
    ----------
    server : PI_server
    
    tags : list
        List with tags as str.
    time_range : tuple
        Tuple with start time and end time as str.
    time_span : str
        Time span (e.g.: '1s', '1d'...)
        
    Returns
    -------
    sample_data : DataFrame
        A pandas DataFrame with the sample data.
    """
    d = {}
    for t in tags:
        tag0 = get_tag(server, t)
        inter_values = interpolated_values(tag0, time_range, time_span)
        d[t] = [v.Value for v in inter_values]

    return pd.DataFrame(d)
