import sys
import logging
logger = logging.getLogger('gen_sonar_images')
if __name__ == '__main__':
	logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s : %(message)s')

logger.info('Importing python modules (takes time for some reason)...')
import holoviews
import dask
import time
import glob

import sonar.sl2_parser
import sonar.map
import sonar.normalized_depth
import sonar.all

# Configure the default renderer
holoviews.extension('bokeh')
renderer = holoviews.renderer('bokeh').instance(mode='server')

def GetFileNames():
	file_names = []
	for arg in sys.argv[1:]:
		fns = glob.glob(arg)
		if len(fns) == 0:
			file_names.append(arg)
		else:
			file_names += fns
	logger.debug('Loading files: %s', file_names)
	return file_names

def GenMapImage(sonar_data):
	try:
		holoviews_map = sonar.map.GenMap(sonar_data)

		# @todo See bug report : https://discourse.holoviz.org/t/holoviews-save-fails-to-export-the-full-the-holoviews-tiles-set/549
		logger.info('Sleeping or bokeh rendering tiles fails to download images before png created')
		time.sleep(20)
		
		map_file_name = sonar_data.attrs['file_name'] + '.map.png'
		logger.info('Saving map image: %s', map_file_name)
		holoviews.save(holoviews_map, map_file_name)
	except:
		logger.exception('Failed to produce map image for: %s', sonar_data.attrs['file_name'])

def GenDepthImage(sonar_data, chan_id, regen_cache=False):
	try:
		depth_image = sonar.normalized_depth.GenDepth(sonar_data, chan_id, regen_cache=regen_cache)

		chan_name = sonar.sl2_parser.ChannelToStr(chan_id)
		depth_file_name = sonar_data.attrs['file_name'] + '.' + str(chan_name) + '.sonar.png'
		logger.info('Saving depth image: %s', depth_file_name)
		holoviews.save(depth_image, depth_file_name)
	except:
		logger.exception('Failed to produce depth images for: %s', sonar_data.attrs['file_name'])


def GenDebugAll(sonar_data, chan_id):
	try:
		plots = sonar.all.GenAll(sonar_data, chan_id)
		
		chan_name = sonar.sl2_parser.ChannelToStr(chan_id)
		for k,plot in plots.items():
			file_name = sonar_data.attrs['file_name'] + '.' + str(chan_name) + '.' + k + '.png'
			logger.info('Saving image: %s', file_name)
			holoviews.save(plot, file_name)
	except:
		logger.exception('Failed to produce depth images for: %s', sonar_data.attrs['file_name'])

def Main():
	regen_cache = False
	file_names = GetFileNames()

	logger.info('Starting dask client')
	dask_client = dask.distributed.Client()
	for file_name in file_names:
		try:
			sonar_data = sonar.sl2_parser.LoadSonarFile(file_name, regen_cache=regen_cache)
			GenMapImage(sonar_data)
			for chan_id in sonar_data.channel.values:
				GenDepthImage(sonar_data, chan_id, regen_cache=regen_cache)
				GenDebugAll(sonar_data, chan_id)
		except:
			logger.exception('Failed to load file: %s', file_name)
			continue
		
	dask_client.close()
	
if __name__ == '__main__':
	Main()
