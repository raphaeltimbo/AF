import clr
import pandas as pd
clr.AddReference('OSIsoft.AFSDK')
clr.AddReference('System.Net')
import OSIsoft.AF as AF
from PI import config
from System.Net import NetworkCredential


__all__ = [
    'AF',
    'get_server',
    'get_tag',
    'interpolated_values',
    'search_tag_mask',
    'search_tag',
    'sample_data'
]


def get_server(server_name, login=None):
    """Connect to server"""
    PI_server = AF.PI.PIServers()[server_name]

    if login is not None:
        PI_server.Connect(
            NetworkCredential(*login),
            AF.PI.PIAuthenticationMode.PIUserAuthentication
        )

    return PI_server


def get_tag(tag_name, server=None):
    """Get a tag.
    
    Parameters
    ----------
    tag_name : str

    server : PI.PIServer, optional
       PI server, if None the config.current server is used.

    """
    if server is None and config.CURRENT_SERVER is None:
        raise ValueError('Pass a server or set "PI.config.current_server"')
    else:
        server = config.CURRENT_SERVER

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


def search_tag_mask(tag_mask, server=None):
    """Search by tag mask.
    
    Parameters
    ----------
    tag_mask : str
        Tag mask (e.g.: *FI*290.033*)
    server : PI.PIServer, optional
       PI server, if None the config.current server is used.

    Returns
    -------
    tags list: list
        List with tags (as str) that match the search.
    """
    if server is None and config.CURRENT_SERVER is None:
        raise ValueError('Pass a server or set "PI.config.current_server"')
    else:
        server = config.CURRENT_SERVER

    tags = AF.PI.PIPoint.FindPIPoints(server, tag_mask)

    return [tag.Name for tag in tags]


def search_tag(tag, server=None):
    """Search by tag mask.

    Parameters
    ----------
    tag_mask : str
        Tag mask (e.g.: *FI*290.033*)
    server : PI.PIServer, optional
       PI server, if None the config.current server is used.

    Returns
    -------
    tags list: list
        List with tags (as str) that match the search.
    tags descriptors : list
        List wit
    """
    if server is None and config.CURRENT_SERVER is None:
        raise ValueError('Pass a server or set "PI.config.current_server"')
    else:
        server = config.CURRENT_SERVER

    tags = AF.PI.PIPoint.FindPIPoints(server, tag, True)
    tag_names = [tag.Name for tag in tags]
    tag_descr = [
        tag.GetAttributes('').__getitem__('descriptor')
        for tag in tags
    ]
    return tag_names, tag_descr


def sample_data(tags, time_range, time_span, save=False, server=None):
    """Get sample data.
    
    Parameters
    ----------
    tags : list
        List with tags as str.
    time_range : tuple
        Tuple with start time and end time as str.
    time_span : str
        Time span (e.g.: '1s', '1d'...)
    server : PI.PIServer, optional
       PI server, if None the config.current server is used.
        
    Returns
    -------
    sample_data : DataFrame
        A pandas DataFrame with the sample data.
    """
    if server is None and config.CURRENT_SERVER is None:
        raise ValueError('Pass a server or set "PI.config.current_server"')
    else:
        server = config.CURRENT_SERVER

    d = {}
    for t in tags:
        tag0 = get_tag(t, server=server)
        inter_values = interpolated_values(tag0, time_range, time_span)
        d[t] = [v.Value for v in inter_values]
    df = pd.DataFrame(d)
    df.columns = [
        i.replace('.', '') for i in
        [j.replace('-', '') for j in df.columns]
    ]

    if save is True:
        filename = (
            'time_range-'
            + time_range[0]
            + ' '
            + time_range[1]
            + '-time_span-'
            + time_span
            + '.df'
        )

        for ch in [':', '/', ' ']:
            if ch in filename:
                filename = filename.replace(ch, '_')
        df.to_pickle(filename)

    return df


