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
logger = logging.getLogger('sonar_depth')

def NormalizeChannelNoCache(data, chan_id):
	channel = data.sel(channel=chan_id)
	if len(channel.depth) == 0:
		logger.error('Invalid channel for normalization')
		return None

	adjusted_min, adjusted_max = channel.upper_limit.min().values.item(), channel.lower_limit.max().values.item()
	adjusted_min = math.floor(adjusted_min)
	adjusted_max = math.ceil(adjusted_max)
	logger.debug('adjusted_min: %s, adjusted_max: %s', adjusted_min, adjusted_max)

	bin_count = len(channel.depth_bin)
	logger.debug('bin_count: %s', bin_count)

	adjusted_depth_per_bin = (adjusted_max - adjusted_min) / bin_count
	logger.debug('adjusted_depth_per_bin: %s', adjusted_depth_per_bin)

	adjusted_bin_depths = [adjusted_min + (j * adjusted_depth_per_bin) for j in range(0, bin_count)]
	logger.debug('adjusted_bin_depths[0]: %s ... [-1]: %s', adjusted_bin_depths[0], adjusted_bin_depths[-1])

	def InterpSingle(unadjusted_depth_amplitudes, unadjusted_min, unadjusted_max, time):
		if (time % 1000) == 0:
			total = len(channel.time)
			perc = 100.0 * time / total
			logger.info('%s : %s of %s', perc, time, total)

		unadjusted_depth_per_bin = (unadjusted_max - unadjusted_min) / bin_count

		min_index = (adjusted_min - unadjusted_min) / unadjusted_depth_per_bin
		max_index = ((adjusted_min + ((bin_count - 1) * adjusted_depth_per_bin)) - unadjusted_min) / unadjusted_depth_per_bin
		index_mapping = numpy.linspace(min_index, max_index, bin_count)
		adjusted_depth_amplitudes = numpy.interp(index_mapping, range(0, len(unadjusted_depth_amplitudes)), unadjusted_depth_amplitudes, left=0, right=0)
		return adjusted_depth_amplitudes

	def Interp(*args, **kwargs):
		#logger.info('args: %s, kwargs: %s', args, kwargs)
		
		data = args[0]
		upper_limit = args[1]
		lower_limit = args[2]
		time = args[3]

		adjusted = []
		for i in range(0, len(upper_limit)):
			d = data[i]
			u = upper_limit[i]
			l = lower_limit[i]
			t = time[i]
			result = InterpSingle(d, u, l, t)
			adjusted.append(result)

		return adjusted

	# For now xarray doesnt work properly, so we will convert to numpy arrays and use
	# apply ufunc to them as it is MUCH faster
	# For more information see: https://github.com/pydata/xarray/issues/3762
	data_arr = channel.data.values
	upper_limit_arr = channel.upper_limit.values
	lower_limit_arr = channel.lower_limit.values
	time_arr = channel.time.values
	normalized = xarray.apply_ufunc(
		Interp, 
		data_arr, 
		upper_limit_arr, 
		lower_limit_arr, 
		time_arr, 
		input_core_dims=[['depth_bin'], [], [], []], 
		output_core_dims=[['depth']],
		
		# We want to use dask and parallelize but it doesnt work in xarray at the moment
		#dask='parallelized',
		#output_dtypes=[numpy.dtype(numpy.int32)],
		#output_sizes={'depth':len(adjusted_bin_depths)},
		)
	x = xarray.Dataset({'amplitudes': (['time', 'depth'], normalized, {'units': 'amplitude'})}, coords={'time': (['time'], time_arr), 'depth': (['depth'], adjusted_bin_depths)})

	return x


def NormalizeChannel(data, chan_id, regen_cache=False):
	normalized_file_name = data.attrs['cache_name'] + '.normalized.chan' + str(chan_id) + '.nc'
	if regen_cache or not os.path.isfile(normalized_file_name):
		try: os.remove(normalized_file_name)
		except: pass
		
		normalized_channel = NormalizeChannelNoCache(data, chan_id)
		if not os.path.isdir(os.path.dirname(normalized_file_name)):
			os.makedirs(os.path.dirname(normalized_file_name))
		normalized_channel.to_netcdf(normalized_file_name)
	normalized_channel = xarray.open_mfdataset(glob.escape(normalized_file_name), combine='by_coords', concat_dim='time')
	return normalized_channel

def GenDepth(sonar_data, chan_id, regen_cache=False):
	normalized_channel = NormalizeChannel(sonar_data, chan_id, regen_cache=regen_cache)
	
	hv_ds = holoviews.Dataset(normalized_channel)
	img = hv_ds.to(holoviews.Image, kdims=["time", "depth"])

	x_size = 1024
	y_size = 768
	rasterized_img = holoviews.operation.datashader.rasterize(img, width=x_size, height=y_size, precompute=True)
	rasterized_img.opts(width=x_size, height=y_size, cmap='viridis', logz=False, invert_yaxis=True)
	return rasterized_img

def GenDepths(sonar_data, regen_cache=False):
	graphs = {}
	for chan_id in sonar_data.channel.values:
		graph = GenDepth(sonar_data, chan_id, regen_cache=regen_cache)
		graphs[chan_id] = graph
	return graphs


