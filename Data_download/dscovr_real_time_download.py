__author__ = 'Harriet Turner'
__email__ = 'h.turner3@pgr.reading.ac.uk'

import os
from datetime import datetime, timedelta
import pandas as pd
import gzip
import shutil
import netCDF4 as nc
from Data_download import useful_functions as f
import numpy as np
from timeit import default_timer as timer


def dscovr_rt_link_generator(link_list, data_product, date):

    """
    Function to find the correct file for the specified date and data product for the DSCOVR spacecraft.
    :param link_list: list of links from a webpage
    :param data_product: the product required from the DSCOVR spacecraft
    :param date: date of the required data
    :return: file name, str
    """

    # turns the date into a string with zeroes if the day/ month is < 10
    date_str = f.date_string(date)
    # the start date of the data file to search for
    start = 's' + date_str + '000000'

    # searching for the file with the correct start date and data product in the file name
    for link in link_list:
        if start in link and data_product in link:
            correct_link = link

    return correct_link


def dscovr_rt_file_downloader(data_product, date, destination):

    """
    Function to download a specified DSCOVR data product for a given date and save it into a specified location.
    :param data_product: data required from DSCOVR
    :param date: date of the data required
    :param destination: location where the data should be saved
    :return:
    """

    # finds the directory that all the data files are in, as there are separate pages for each month
    parent_page = f.rt_directory_finder(date, master_page='https://www.ngdc.noaa.gov/dscovr/data/')
    # gets the url for the data
    url = dscovr_rt_link_generator(f.webpage_links(parent_page), data_product, date)
    # downloads the data
    f.web_scraper(str(parent_page + '/' + url), os.path.join(destination, url))

    # the file name of the saved data
    gzip_file = os.path.join(destination, url)
    # unzips the gzip file
    with gzip.open(gzip_file, 'rb') as f_in:
        with open(gzip_file[:-3], 'wb') as f_out:
            # copies the files inside the zip file into the parent folder
            shutil.copyfileobj(f_in, f_out)

    # removes the zip file
    os.remove(gzip_file)


def dscovr_time(list):

    """
    Function to change a list of DSCOVR dates into a datetime format. The DSCOVR time is in milliseconds since
    1970-01-01 00:00:00.
    :param list: input times
    :return: list of datetime objects
    """

    # empty list for the dates
    dates = []

    # goes through the given list and converts the date from UNIX time to a datetime object
    for item in list:
        date = datetime(1970, 1, 1, 0, 0, 0) + timedelta(milliseconds=item)
        dates.append(date)

    # returns the date in a datetime format
    return dates


def dscovr_sw_speed(list):

    """
    Function to change the arrays of DSCOVR solar wind speed into a single list.
    :param list: input speeds
    :return: list of speeds
    """

    # empty list for the speeds
    speeds = []

    # loops through the speeds
    for speed in list:
        speeds.append(speed)

    return speeds


def dscovr_netcdf_reader(file):

    """
    Function to read the DSCOVR data from a Net CDF file.
    :param file: path to the file, str
    :return: date/ time data and the proton speed data
    """

    # testing to make sure the correct data product is being used
    if 'fc0' in file:
        raise ValueError("Does not work for this data product. Use fc1 or f1m instead.")

    # reading the data from the file
    data = nc.Dataset(file)

    # returning the date column and proton speed column
    return dscovr_time(data['time'][:].data), dscovr_sw_speed(-1 * (data['proton_vx_gse'][:].data))


def dscovr_obs_download(start_date, end_date, obs_folder):

    """
    Function to download DSCOVR real time data between two dates to feed into BRAvDA in the correct format.
    :param start_date: start of the interval for the data download
    :param end_date: end of the interval for the data download
    :param obs_folder: where the observations are saved
    :return:
    """

    # creates a list of daily dates between the start and end dates
    dates = f.date_list(start_date, end_date-timedelta(days=1), delta=timedelta(days=1))

    # turns the date into a string to find the correct folder
    date_str = f.date_string(start_date)

    # checking if there is the raw data folder for the data to be saved in and creates it if it doesn't
    if os.path.exists(os.path.join(obs_folder, 'DSCOVR_raw')) == True:
        pass
    else:
        os.mkdir(os.path.join(obs_folder, 'DSCOVR_raw'))

    # creates a folder for the data to be saved in, with the name YYYYMMDD of the given date
    # checks first to see if the folder exists, so it doesn't error by trying to make a new one
    if os.path.exists(os.path.join(obs_folder, 'DSCOVR_raw', date_str)) == True:
        print('Folder exists for this date. Removing folder contents.')
        files = os.listdir(os.path.join(obs_folder, 'DSCOVR_raw', date_str))
        for file in files:
            os.remove(os.path.join(obs_folder, 'DSCOVR_raw', date_str, file))
    else:
        os.mkdir(os.path.join(obs_folder, 'DSCOVR_raw', date_str))

    # downloading the data for each day in the window and saving it into the folder created above
    # tries to download the science data first, if not available then tries the less processed data
    # if there is no data available, it prints out a message saying there is no data
    for obs_date in dates:
        try:
            dscovr_rt_file_downloader('f1m', obs_date, os.path.join(obs_folder, 'DSCOVR_raw', date_str))
        except:
            try:
                dscovr_rt_file_downloader('fc1', obs_date, os.path.join(obs_folder, 'DSCOVR_raw', date_str))
            except:
                print('Data not available for', obs_date)
                continue


def dscovr_obs_format(start_date, directory):

    """
    Function to take the downloaded observations and change them into a format that can be used by BRaVDA.
    :param start_date: date of the start of the data window being downloaded, datetime object
    :param directory: location of the folder containing the observation files, str
    :return:
    """

    # transforming the given date into strings to find the correct folder
    date_str = f.date_string(start_date)

    # creates the file name from the string of the given date
    folder = os.path.join(directory, 'DSCOVR_raw', date_str)
    # lists all the files in the folder
    files = os.listdir(folder)
    # makes sure the files are sorted by their start date, so they are in chronological order
    files.sort(key=f.dscovr_file_sort_key)

    # empty lists to append the data to from each file
    dates = []
    sw_speed = []

    # loops through the files in the folder and extracts the data
    for file in files:
        file_path = os.path.join(folder, file)
        dates.append(dscovr_netcdf_reader(file_path)[0])
        sw_speed.append(dscovr_netcdf_reader(file_path)[1])

    # flattens the list of lists into a single list for the dates and solar wind speeds
    dates = f.flatten_list(dates)
    sw_speed = f.flatten_list(sw_speed)

    # turning the values that are unphysical into NaNs
    for i in range(0, len(sw_speed)):
        if sw_speed[i] > 5000 or sw_speed[i] < 0:
            sw_speed[i] = np.nan
        else:
            sw_speed[i] = sw_speed[i]

    # puts the data into a pandas dataframe
    df = pd.DataFrame()
    df['Date'] = dates
    df['Solar_wind_speed'] = sw_speed

    # makes a list of hourly dates from the first and last dates in the dataframe
    hourly_dates = f.date_list(dates[0], dates[-1], timedelta(hours=1))

    # creates lists of year, DOY and hour for the data that feeds into BRaVDA
    year = []
    doy = []
    hour = []
    for hourly_date in hourly_dates:
        year.append(f.date_to_doy(hourly_date)[0])
        doy.append(f.date_to_doy(hourly_date)[1])
        hour.append(f.date_to_doy(hourly_date)[2])

    # empty lists to append the hourly averaged data to
    hourly_averaged_sw_speed = []

    # looping through the data to average it all to one hour resolution for use in BRaVDA
    for i in range(0, len(hourly_dates)):
        data_req = df.loc[(df['Date'] >= (hourly_dates[i] - timedelta(minutes=30))) &
                          (df['Date'] < (hourly_dates[i] + timedelta(minutes=30)))]
        average_speed = data_req['Solar_wind_speed'].mean()
        hourly_averaged_sw_speed.append(average_speed)

    # creates a dataframe of the hourly averaged data
    averaged_df = pd.DataFrame()
    averaged_df['Year'] = year
    averaged_df['DOY'] = doy
    averaged_df['Hour'] = hour
    averaged_df['Solar_wind_speed'] = hourly_averaged_sw_speed

    # saves the data in a text file
    np.savetxt(os.path.join(directory, 'DSCOVR_rt_observations.txt'), averaged_df.values,
               fmt=['%4.0d', '%4.0d', '%3.0d', '%6.0f'])


def dscovr_real_time_obs(start_date, end_date, directory):

    """
    Function that pulls together the parts to download and format the real time observations from DSCOVR to use in
    BRaVDA.
    :param start_date: start date of the observations required
    :param end_date: end date of the observations required
    :param directory: location where the data will be saved
    :return:
    """

    # starting a timer to see how long the code takes
    timer_start = timer()

    # using the function to download the observations
    dscovr_obs_download(start_date, end_date, directory)

    # using the function to format the observations that have just been downloaded
    dscovr_obs_format(start_date, directory)

    # stopping the timer and printing out the time it took for the code to run
    timer_end = timer()
    print(timer_end - timer_start, 'seconds to download and format DSCOVR data.')

    return None


if __name__ == '__main__':
    dscovr_real_time_obs(datetime(2023, 7, 15), datetime(2023, 7, 20), 'D:\\PhD\\Real_time_data_download\\Data')

