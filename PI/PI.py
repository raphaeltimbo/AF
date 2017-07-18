import clr
import pandas as pd
clr.AddReference('OSIsoft.AFSDK')
clr.AddReference('System.Net')
import OSIsoft.AF as AF
from PI import config
from System.Net import NetworkCredential
from tqdm import tqdm


__all__ = [
    'AF',
    'get_server',
    'get_tag',
    'interpolated_values',
    'search_tag_mask',
    'search_tag',
    'sample_data',
    'sample_big_data',
    'save_df'
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


def save_df(df, filename=None):
    if filename is None:
        start = df.index[0]
        end = df.index[-1]
        filename = (
            f'{start.day}-{start.month}-{start.year}'
            + f'--{end.day}-{end.month}-{end.year}'
            + f'{end.freq.name}'
            + f'.df'
        )

        for ch in [':', '/', ' ']:
            if ch in filename:
                filename = filename.replace(ch, '_')
        df.to_pickle(filename)
    print(f'Saved as {filename}')


def sample_data(tags, time_range, time_span, save_data=False, server=None):
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
    PIAttributes = {}
    tagAttributes = {}

    for t in tags:
        tag0 = get_tag(t, server=server)
        inter_values = interpolated_values(tag0, time_range, time_span)
        d[t] = [v.Value for v in inter_values]
        # create dictionary with descriptors
        for descr in tag0.GetAttributes(''):
            tagAttributes[str(descr.Key)] = str(descr.get_Value())
        PIAttributes[str(tag0.Name)] = tagAttributes

        # set date_range index

    f = config.FREQUENCY[time_span]
    p = len(d[tags[0]])
    index = pd.date_range(
         start=pd.to_datetime(time_range[0], dayfirst=True), periods=p, freq=f
    )

    try:
        df = pd.DataFrame(d, index=index)
    except ValueError as exc:
        df = pd.DataFrame(d)
        print('Index was not applied: ', exc)

    # remove . and - so that tags are available with using 'df.'
    df.columns = [
        i.replace('.', '') for i in
        [j.replace('-', '') for j in df.columns]
    ]

    old_keys = list(PIAttributes.keys())
    for k in old_keys:
        new_key = k.replace('.', '').replace('-', '')
        PIAttributes[new_key] = PIAttributes.pop(k)
    for col in df.columns:
        setattr(getattr(df, col), 'PIAttributes', PIAttributes[col])

    # eliminate errors such as 'comm fail' before resampling
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    if save_data is True:
        save_df(df)

    return df


def sample_big_data(tags, time_range, time_span, save_data=False, server=None):
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
    # change to pandas datetime to split the calls to PI
    start = pd.to_datetime(time_range[0], dayfirst=True)
    end = pd.to_datetime(time_range[1], dayfirst=True)

    f = config.FREQUENCY[time_span]
    date_range = pd.date_range(start, end, freq=f)

    ch = 1000  # number of 1000s chunks
    if divmod(len(date_range), ch)[1] < 2:  # avoid final step with 1 (st=end)
        rng = len(date_range) // (ch + 1)
    else:
        rng = len(date_range) // ch

    if rng == 0:
        return sample_data(tags=tags, time_range=time_range, time_span=time_span, save_data=save_data)

    for i in tqdm(range(rng), desc='Getting Data'):
        start = date_range[1000 * i]
        end = date_range[(1000 * (i + 1) - 1)]
        # go back to PI string format before getting the data
        st = start.strftime('%d/%m/%Y %H:%M:%S')
        en = end.strftime('%d/%m/%Y %H:%M:%S')
        time_range_pi = (st, en)
        if i == 0:
            df0 = sample_data(tags, time_range_pi, time_span, server=server)
        else:
            df1 = sample_data(tags, time_range_pi, time_span, server=server)
            df0 = df0.append(df1)

    # last step
    start = date_range[1000 * (i + 1)]
    end = date_range[-1]
    # go back to PI string format before getting the data
    st = start.strftime('%d/%m/%Y %H:%M:%S')
    en = end.strftime('%d/%m/%Y %H:%M:%S')
    time_range_pi = (st, en)

    df1 = sample_data(tags, time_range_pi, time_span, server=server)
    df0 = df0.append(df1)  # we lose the frequency with append

    df0 = df0.resample(f).mean()  # get the frequency back with resample

    if save_data is True:
        save_df(df0)

    return df0
