__author__ = 'Harriet Turner'
__email__ = 'h.turner3@pgr.reading.ac.uk'

from Data_download import dscovr_real_time_download as dscovr
from Data_download import stereoa_real_time_download as sta
from datetime import datetime

# Downloading the data from the spacecraft into the Data directory

dscovr.dscovr_real_time_obs(datetime(2023, 7, 15),
                            datetime(2023, 7, 20),
                            'D:\\PhD\\Real_time_data_download\\Data')

sta.stereoa_real_time_obs(datetime(2023, 7, 15),
                          datetime(2023, 7, 20),
                          'D:\\PhD\\Real_time_data_download\\Data')
