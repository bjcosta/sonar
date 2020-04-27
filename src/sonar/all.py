import sys
import os
import os.path
import math
import glob

import numpy
import xarray
import holoviews
import holoviews.operation.datashader
import sonar.sl2_parser

import logging
logger = logging.getLogger('sonar_all')


def GenAll(sonar_data, chan_id):
	channel = sonar_data.sel(channel=chan_id)

	# From: http://holoviews.org/reference/elements/bokeh/Path.html
	#N, NLINES = 100, 10
	#paths = hv.Path((np.arange(N), np.random.rand(N, NLINES) + np.arange(NLINES)[np.newaxis, :]))
	#paths2 = hv.Path((np.arange(N), np.random.rand(N, NLINES) + np.arange(NLINES)[np.newaxis, :]))
	#
	#overlay = paths * paths2
	#overlay.opts(width=600)

	#x_range = (channel.time[0], channel.time[-1])
	#y_range = (channel.depth.min().compute().item(), channel.depth.max().compute().item())
	#import datashader
	#canvas = datashader.Canvas(x_range=x_range, y_range=y_range, plot_height=300, plot_width=900)
	#img = datashader.transfer_functions.shade(channel.depth)
	
	import datashader.transfer_functions
	#channel_df = channel.to_dataframe()

	plots = {}
	for k in channel.keys():
		# Cant plot 2d data in here
		if k == 'data': continue
		
		#if k not in ['altitude', 'temperature', 'speed_water', 'speed_gps', 'unknown6']: continue

		# From: http://holoviews.org/user_guide/Tabular_Datasets.html
		#logger.info('Creating graph for: %s', k)
		v = getattr(channel, k)
		t = holoviews.Table((channel.time, v), 'time', k)
		c = holoviews.Curve(t)
		plots[k] = c
		
	
	#import code
	#code.interact(local=dict(globals(), **locals()))
	#holoviews.Path(channel.

	# From: https://stackoverflow.com/questions/42162419/what-is-the-best-method-for-using-datashader-to-plot-data-from-a-numpy-array
	#agg = canvas.line(channel, 'depth', 'time', datashader.count())
	#agg = canvas.line(channel_df, 'depth', 'time', datashader.count())
	#img = datashader.transfer_functions.shade(agg, how='eq_hist')




	
	# Default plot ranges:
	#x_range = (df.iloc[0].ITime, df.iloc[-1].ITime)
	#y_range = (1.2*signal.min(), 1.2*signal.max())
	#print("x_range: {0} y_range: {0}".format(x_range,y_range))


	# For each item in the dataset, we want to generate a set of Paths() to plot
	#hv_ds = holoviews.Dataset(channel)
	#img = hv_ds.to(holoviews.Image, kdims=["time", "depth"])

	#x_size = 1024
	#y_size = 768
	#rasterized_img = holoviews.operation.datashader.rasterize(img, width=x_size, height=y_size, precompute=True)
	#rasterized_img.opts(width=x_size, height=y_size, cmap='viridis', logz=False, invert_yaxis=True)
	#return rasterized_img
	return plots
