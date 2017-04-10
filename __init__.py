"""PI AF SDK

Module to give a python interface to PI AF SDK.

Example:

Get the PI server:

>>> PI_server = PI.get_server('pi-rnce')

Find a tag from tag mask:

>>> tags = PI.search_tag_mask(PI_server, '*VI*290.003*')

>>> for tag in tags:
...     print(tag)
VI-290.003X
VI-290.003Y

>>> time_range = ('26/03/2017 10:00:00', '26/03/2017 11:00:00')
>>> time_span = '1s'
>>> df = PI.sample_data(PI_server, tags, time_range, time_span)

"""

from .PI import *

