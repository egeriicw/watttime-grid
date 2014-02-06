import os
import sys
import requests
import zipfile
import StringIO

# path from ipynb to data
data_dir = 'data/isos/miso'

def download_historical_interchange(yr, mo):
    """download data and put in directory structure"""    
    # set up
    zipfname = '%s%s_sr_is_xls.zip' % (yr, mo)
    url = os.path.join('https://www.misoenergy.org//MKTRPT_Archives/sr_is/',
                       zipfname)
            
    # get
    response = requests.get(url)
    print response.url

    # unzip into target directory
    zipf = zipfile.ZipFile(StringIO.StringIO(response.content))
    zipf.extractall(path=data_dir)
    zipf.close()

if __name__ == "__main__":
    yrs = sys.argv[1].split(',')
    mos = ['%02d' % i for i in range(1,13)]
    
    # download all interchange data
    for yr in yrs:
        for mo in mos:
            download_historical_interchange(yr, mo)
    