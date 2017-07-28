import clr
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pickle
clr.AddReference('OSIsoft.AFSDK')
clr.AddReference('System.Net')
import OSIsoft.AF as AF
from PI import config
from System.Net import NetworkCredential
from tqdm import tqdm


__all__ = [
    'AF',
    'PIDataFrame',
    'get_server',
    'get_tag',
    'interpolated_values',
    'search_tag_mask',
    'search_tag',
    'sample_data',
    'sample_big_data',
    'save_df',
    'save_to_pandas',
    'load_from_pickle'
]


class PIDataFrame(pd.DataFrame):

    _metadata = ['PIAttributes']

    @property
    def _constructor(self):
        return PIDataFrame


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
    tag : TAG object or str

    time_range : tuple
        Tuple with start time and end time as str.
    time_span : str
        Time span (e.g.: '1s', '1d'...)
        
    Returns
    -------
    interpolated_values
    
    """
    if not isinstance(tag, AF.PI.PIPoint):
        tag = get_tag(tag)
    time_range_pi = AF.Time.AFTimeRange(*time_range)
    time_span_pi = AF.Time.AFTimeSpan.Parse(time_span)

    values = None
    n_of_tries = 0

    while values is None and n_of_tries < 3:
        try:
            values_pi = tag.InterpolatedValues(time_range_pi, time_span_pi, '', '')
            values = [v.Value for v in values_pi]
        except AF.PI.PITimeoutException:
            n_of_tries += 1
            print(f'PITimeout -> Number of tries: {n_of_tries}')
            pass
        except AF.PI.PIException:
            print(f'Error when trying to get interpolated values for '
                  f'{tag}, {time_range}, {time_span}')
            break

    if values is None:
        f = config.FREQUENCY[time_span]
        number_of_samples = len(pd.date_range(
            *pd.to_datetime(time_range, dayfirst=True), freq=f))
        values = [np.nan for _ in range(number_of_samples)]

    return values


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

        with open(filename, 'wb') as f:
            pickle.dump([df, df.PIAttributes], f)

    print(f'Saved as {filename}')


def save_to_pandas(file):
    """Saves to pandas DataFrame.
    It will save PIAttributes separate from the pandas DataFrame.

    Parameters
    ----------
    file : file saved with save_df
    """
    # load df
    df = load_from_pickle(file)
    # separate
    PIAttributes = df.PIAttributes
    df = pd.DataFrame(df)
    # save
    print(f'Saved as {file + "pd"}')
    with open((file + 'pd'), 'wb') as f:
        pickle.dump([df, PIAttributes], f)


def load_from_pickle(filename):
    """Load df from pickle.

    This function will load the dataframe and the metadata
    associated with PIAttributes.

    Parameters
    ----------
    filename : str
        File name.

    Returns
    -------
    df : DataFrame
        A pandas DataFrame with the sample data.
    """
    with open(filename, 'rb') as f:
        df, PIAttributes = pickle.load(f)

    df.PIAttributes = PIAttributes

    return df


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

    for t in tags:
        tag0 = get_tag(t, server=server)
        d[t] = interpolated_values(tag0, time_range, time_span)
        # create dictionary with descriptors
        tagAttributes = {}
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
        df = PIDataFrame(d, index=index)
    except ValueError as exc:
        df = PIDataFrame(d)
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
    df.PIAttributes = PIAttributes

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

    chunks = {'S': 1000, 'H': 100, 'D': 10}
    ch = chunks[f]  # number of chunks

    if divmod(len(date_range), ch)[1] < 2:  # avoid final step with 1 (st=end)
        rng = len(date_range) // (ch + 1)
    else:
        rng = len(date_range) // ch

    if rng == 0:
        return sample_data(tags=tags, time_range=time_range, time_span=time_span, save_data=save_data)

    for i in tqdm(range(rng), desc='Getting Data'):
        start = date_range[ch * i]
        end = date_range[(ch * (i + 1) - 1)]
        # go back to PI string format before getting the data
        st = start.strftime('%d/%m/%Y %H:%M:%S')
        en = end.strftime('%d/%m/%Y %H:%M:%S')
        time_range_pi = (st, en)
        if i == 0:
            df0 = sample_data(tags, time_range_pi, time_span, server=server)
            # store PIAttributes to avoid losing them after append
            PIAttributes = df0.PIAttributes
        else:
            df1 = sample_data(tags, time_range_pi, time_span, server=server)
            df0 = df0.append(df1)

    # last step
    start = date_range[ch * (i + 1)]
    end = date_range[-1]
    # go back to PI string format before getting the data
    st = start.strftime('%d/%m/%Y %H:%M:%S')
    en = end.strftime('%d/%m/%Y %H:%M:%S')
    time_range_pi = (st, en)

    df1 = sample_data(tags, time_range_pi, time_span, server=server)
    df0 = df0.append(df1)  # we lose the frequency with append

    df0 = df0.resample(f).mean()  # get the frequency back with resample

    df0.PIAttributes = PIAttributes

    if save_data is True:
        save_df(df0)

    return df0


def PI_plot(tags, df, PIAttributes, ax=None):
    """Plot PI values.

    Parameters
    ----------
    df : pd.DataFrame

    tags : str
        String with the tags (e.g.: 'VI290003X VI290003Y')
    ax : matplotlib.axes, optional
        Matplotlib axes where data will be plotted.
        If None creates a new.

    returns
    ax : matplotlib.axes
        Matplotlib axes with plotted data.
    """
    if ax is None:
        fig, ax = plt.subplots(figsize=(16, 8))
    tags = tags.split(' ')
    tagunits = {}

    # checke how many units/axes
    for tag in tags:
        # check if unit is already in tagunits
        if not PIAttributes[tag]['engunits'] in tagunits.values():
            # create unit
            tagunits[tag] = PIAttributes[tag]['engunits']

    n_units = len(tagunits)
    units = [i for i in tagunits.values()]

    if n_units > 3:
        raise Exception('Cannot plot more than 3 units')

    if n_units == 1:
        for tag in tags:
            series = getattr(df, tag)
            ax.plot(series, label=tag)
            ax.legend()
    else:
        axes = [ax.twinx() for i in range(len(tagunits) - 1)]
        axes.insert(0, ax)

    if n_units == 2:
        # set the labels
        for _ax, unit in zip(axes, tagunits.values()):
            _ax.set_ylabel(unit)

        # check unit for each tag and plot to the correct axes
        lines = []
        labels = []
        for tag in tags:
            unit = PIAttributes[tag]['engunits']
            idx = units.index(unit)
            series = getattr(df, tag)
            # Make solid lines for lines in ax0
            if idx == 0:
                line, = axes[idx].plot(series, label=tag)
            else:
                line, = axes[idx].plot(series, linestyle='--', alpha=0.3, label=tag)
            lines.append(line)
            labels.append(line.get_label())
        ax = axes[0]
        box = ax.get_position()
        # ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])
        ax.legend(lines, labels, loc='center left', bbox_to_anchor=(1.05, 0.5))
        # ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])

    if n_units == 3:
        # set the labels
        for _ax, unit in zip(axes, tagunits.values()):
            _ax.set_ylabel(unit)

        # check unit for each tag and plot to the correct axes
        lines = []
        labels = []
        for tag in tags:
            unit = PIAttributes[tag]['engunits']
            idx = units.index(unit)
            series = getattr(df, tag)
            # Make solid lines for lines in ax0 and keep one color cycle
            next_color = axes[0]._get_lines.get_next_color()
            if idx == 0:
                line, = axes[idx].plot(series, label=tag, color=next_color)
            else:
                line, = axes[idx].plot(series, linestyle='--', color=next_color, alpha=0.5, label=tag)
            lines.append(line)
            labels.append(line.get_label())
        ax = axes[0]
        box = ax.get_position()
        ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])
        ax.legend(lines, labels, loc='center left', bbox_to_anchor=(1.2, 0.5))

        # Make some space on the right side for the extra y-axis.
        fig.subplots_adjust(right=0.75)

        # Move the last y-axis spine over to the right by 20% of the width of the axes
        axes[-1].spines['right'].set_position(('axes', 1.1))

        # To make the border of the right-most axis visible, we need to turn the frame
        # on. This hides the other plots, however, so we need to turn its fill off.
        axes[-1].set_frame_on(True)
        axes[-1].patch.set_visible(False)


