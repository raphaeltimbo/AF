import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import PI as PI
import pickle
from tqdm import tqdm

PI.config.CURRENT_SERVER = PI.get_server('pi-utpf', login=('pidemo', ''))

tags = [
    'FI-290.033.PV',
    'FI-290.067',
    'FV-290.011',
    'LI-290.005.PV',
    'LI-290.028.PV',
    'LI-290.029.PV',
    'PDI-290.008',
    'PDI-290.011',
    'PDI-290.012',
    'PDI-290.017',
    'PDIC-290.012.05',
    'PI-240.040',
    'PI-290.006.PV',
    'PI-290.032.PV',
    'PI-290.062.PV',
    'PI-290.063.PV',
    'PI-290.067.PV',
    'PIC-290.005.PV',
    'SI-290.003.PV',
    'TI-290.017.PV',
    'TI-290.018.PV',
    'TI-290.019.PV',
    'TI-290.020.PV',
    'TI-290.021.PV',
    'TI-290.022.PV',
    'TI-290.023.PV',
    'TI-290.028.PV',
    'TI-290.091A.PV',
    'TI-290.092A.PV',
    'TIC-290.027.PV',
    'VI-290.003X',
    'VI-290.003Y',
    'VI-290.004X',
    'VI-290.004Y',
    'VI-290.003X_Not1X',
    'VI-290.003Y_Not1X',
    'VI-290.004X_Not1X',
    'VI-290.004Y_Not1X'
    ]

time_span = '1d'
time_range = ('01/01/2015 01:00:00', '31/12/2015 01:00:00')

df1 = PI.sample_big_data(tags, time_range, time_span)
