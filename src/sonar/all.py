import sys
import os
import os.path
import math
import glob

import numpy
import xarray
import holoviews
import holoviews.operation.datashader

import logging
logger = logging.getLogger('sonar_all')


def GenAll(sonar_data, chan_id=None):
    '''Used for debugging, plots a single 2D line graph for each item decoded from the sl2 file'''
    
    if chan_id is None:
        # We need to create a single dimension of frame_index not have a second dimension of channel

        # @todo This isnt working at the moment. Stack is wrong way to do this
        assert(False)

        # stack cant modify existing dimension, so rename
        channel = sonar_data.rename({'frame_index':'frame_index_orig'})
        
        # We stack the channel and frame_index_orig dimensions into a single dimension called frame_index
        channel = channel.stack(frame_index=['frame_index_orig', 'channel'])
    else:
        channel = sonar_data.sel(channel=chan_id)

    plots = {}
    for k in channel.keys():
        # Cant plot 2d data in here so skip the sonar full return data "image"
        if k == 'data': continue
        
        # From: http://holoviews.org/user_guide/Tabular_Datasets.html
        v = getattr(channel, k)
        t = holoviews.Table((channel.frame_index, v), 'frame_index', k)
        c = holoviews.Curve(t)
        plots[k] = c

    return plots

