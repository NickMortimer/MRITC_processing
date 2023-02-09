#stills qtc

import doit
from doit import get_var
from doit.tools import run_once
from doit import create_after
import glob
from matplotlib.pyplot import axes
import yaml
import os
import pandas as pd
import shutil
import config
from config import task_read_config
import re
import cv2
from skimage import exposure
import xarray as xr
import numpy as np


def read_hist(filename):
    src = cv2.imread(filename)
    hist = cv2.calcHist(src, [0, 1, 2], None, [8, 8, 8],
        [0, 256, 0, 256, 0, 256])
    hist = cv2.normalize(hist, hist).flatten()
    return hist

def task_make_QC_histograms():

        def histqal(dependencies, targets):
            hists = np.vstack([ read_hist(file) for file in dependencies])
            result =xr.DataArray(hists,dims=['file','bands'],coords={'file':dependencies,'bands':range(0,512)},name='hists').to_dataset()
            result.to_netcdf(targets[0])


        file_dep = glob.glob(os.path.join(config.cfg['paths']['goodimagesource'],os.path.join(config.cfg['paths']['imagewild'])))
        target = os.path.join(config.basepath,config.cfg['paths']['process'],config.cfg['paths']['sourcehists'])
        return {
            'file_dep':file_dep,
            'actions':[histqal],
            'targets':[target],
            'uptodate':[True],
            'clean':True,
        }


# def task_create_image_histograms():
#         def hists(dependencies, targets,filter):
#             files =glob.glob(filter)
#             hists = np.vstack([ read_hist(file) for file in files])
#             result =xr.DataArray(hists,dims=['file','bands'],coords={'file':files,'bands':range(0,512)},name='hists').to_dataset()
#             result.to_netcdf(targets[0])    
#         vidname = re.compile('IN\d{4}_V\d{2}_\d{3}')
#         for item in glob.glob(os.path.join(config.basepath,config.cfg['paths']['imagesource']),recursive=True):
#             if glob.glob(os.path.join(item,config.cfg['paths']['imagewild'].upper())) or os.path.join(item,config.cfg['paths']['videowild'].lower()):
#                 target  = os.path.join(config.basepath,config.cfg['paths']['process'],f'{vidname.search(item).group(0)}_histogram.nc')
#                 filter = os.path.join(item,config.cfg['paths']['imagewild'])
#                 file_dep = glob.glob(filter)
#                 if file_dep:
#                     yield { 
#                         'name':item,
#                         'actions':[(hists,[],{'filter':filter})],
#                         'targets':[target],
#                         'uptodate':[True],
#                         'clean':True,
#                     } 

# goodimage = '/mnt/science/science/DTC/DTC_StillsQC/Good_DTC_DSP_IN2022_V09_142_20221212T054208Z_00177.JPG'
# badimage = '/mnt/science/science/DTC/DTC_StillsQC/DTC_DSP_IN2022_V09_004_20221122T024316Z_00131.JPG'
# test = '/mnt/science/science/DTC/DTC_StillsQC/DTC_DSP_IN2022_V09_004_20221122T024318Z_00132.JPG'


def task_compair_image_histograms():
        def compare_hist(a,b):
            return cv2.compareHist(a, b, cv2.HISTCMP_CORREL)
        def hists(dependencies, targets,source):
            hists =xr.open_dataset(dependencies[0])
            qhists = xr.open_dataset(source)
            result =[]
            for file in range(0,len(qhists.file)):
                result.append(xr.apply_ufunc(compare_hist,hists.hists,qhists.hists.isel(file=file),input_core_dims=[['bands'],['bands']],vectorize=True))
            data =xr.concat(result,dim='quality').to_dataset()
            data['qaulity'] = xr.DataArray(qhists.file,dims=['quality'],coords={'quality':range(0,len(qhists.file))})
            

        file_dep = glob.glob(os.path.join(config.basepath,config.cfg['paths']['process'],'*_histogram.nc'))
        source = os.path.join(config.basepath,config.cfg['paths']['process'],config.cfg['paths']['sourcehists'])
        for item in file_dep:
            target = item.replace('_histogram.nc','_histogram.csv')
            yield { 
                'name':item,
                'file_dep':[item],
                'actions':[(hists,[],{'source':source})],
                'targets':[target],
                'uptodate':[True],
                'clean':True,
            } 





# ref = cv2.imread(badimage)
# refhist = cv2.calcHist(ref, [0, 1, 2], None, [8, 8, 8],
#     [0, 256, 0, 256, 0, 256])
# refhist = cv2.normalize(refhist, refhist).flatten()

# d = cv2.compareHist(hist, refhist, cv2.HISTCMP_CORREL)



# multi = True if src.shape[-1] > 1 else False
# matched = exposure.match_histograms(src, ref, multichannel=multi)
# cv2.imshow("Source", src)
# cv2.imshow("Reference", ref)
# cv2.imshow("Matched", matched)
# cv2.waitKey(0)



if __name__ == '__main__':
    import doit
    DOIT_CONFIG = {'check_file_uptodate': 'timestamp'}
    #print(globals())
    doit.run(globals())