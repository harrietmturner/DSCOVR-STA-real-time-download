__author__ = 'Harriet Turner'
__email__ = 'h.turner3@pgr.reading.ac.uk'

import os
import cdflib
from datetime import datetime, timedelta
import pandas as pd
from Data_download import useful_functions as f
import numpy as np
from timeit import default_timer as timer


def cdf_link_filter(link_list):

    """
    Function to filter out the links that end with '.cdf' from a list.
    :param link_list: list of links that need to be filtered
    :return: list of the filtered links
    """

    # empty list for the specific links
    cdf_links = []

    # goes through the given list of links to find the ones ending in '.cdf' abd containing the word 'BROWSE'
    for link in link_list:
        if link[-4:] == '.cdf' and 'BROWSE' in link:
            cdf_links.append(link)

    return cdf_links


def cdf_link_date_filter(link_list, date):

    """
    Function to filter out the links ending in '.cdf' from a list for a given date
    :param link_list: list of links that needs to be filtered
    :param date: the date of the data file required
    :return: list of filtered links
    """

    # turns the date into a string
    date_str = f.date_string(date)

    # searches through the link list to find the links that contain the given date, '.cdf' and 'BROWSE'
    for link in link_list:
        if link[-4:] == '.cdf' and 'BROWSE' in link and date_str in link:
            cdf_link = link

    # returns the required link
    return cdf_link


def stereoa_rt_file_downloader(date, destination):

    """
    Function to download the STEREO-A real time data from the PLASTIC instrument.
    :param date: date of the required data
    :param destination: where the data file will be saved
    :return: data saved in the specified location
    """

    # finding the webpage for the month and year
    parent_directory = 'https://stereo-ssc.nascom.nasa.gov/data/beacon/ahead/plastic/'
    master_link = f.rt_directory_finder(date, parent_directory)
    # listing all the links on the webpage
    links = f.webpage_links(master_link)
    # finding the correct cdf link for the date
    url = cdf_link_date_filter(links, date)

    # using a webscraper to download the data and save it to the destination
    f.web_scraper(str(master_link + '/' + url), os.path.join(destination, url))


def stereoa_obs_download(start_date, end_date, obs_folder):

    """
    Function to download the STEREO-A real time data between two dates. Creates a folder to store the data in.
    :param start_date: start date of the interval to be downloaded
    :param end_date: end date of the interval to be downloaded
    :param obs_folder: where the data will be saved
    :return:
    """

    # creates a list of dates up to the date given
    dates = f.date_list(start_date, end_date - timedelta(days=1), delta=timedelta(days=1))

    # creating a string from the date
    date_str = f.date_string(start_date)

    # makes a folder for the raw files, if it doesn't already exist
    if os.path.exists(os.path.join(obs_folder, 'STEREO-A_raw')) == True:
        pass
    else:
        os.mkdir(os.path.join(obs_folder, 'STEREO-A_raw'))

    # checks if the folder already exists
    # if it does, then the contents are removed so that the new data can be downloaded into the folder
    # if it doesn't exists, then it is created
    if os.path.exists(os.path.join(obs_folder, 'STEREO-A_raw', date_str)) == True:
        print('Folder exists for this date. Removing folder contents.')
        files = os.listdir(os.path.join(obs_folder, 'STEREO-A_raw', date_str))
        for file in files:
            os.remove(os.path.join(obs_folder, 'STEREO-A_raw', date_str, file))
    else:
        os.mkdir(os.path.join(obs_folder, 'STEREO-A_raw', date_str))

    # downloading the data and saving it into the folder that has just been created/ emptied
    # loops through the list of dates between the start and end of the interval, so that a data file is downloaded
    # for each day
    # if the data file cannot be downloaded, it is skipped
    for date in dates:
        try:
            stereoa_rt_file_downloader(date, os.path.join(obs_folder, 'STEREO-A_raw', date_str))
        except:
            continue


def stereoa_cdf_reader(file):

    """
    Function to read the cdf file given and return a pandas dataframe.
    :param file: filepath to the cdf file
    :return:
    """

    # making sure that the data file is the correct type
    if '.cdf' not in file:
        raise ValueError('File is of the wrong type. It must be a .cdf file.')

    # empty lists for the time and the solar wind speed
    time = list()
    solar_wind_speed = list()

    # reading the cdf file
    cdf = cdflib.CDF(file)
    # converting the time into a datetime object
    raw_time = cdflib.cdfepoch.breakdown(cdf['Epoch1'])

    # appending the solar wind speed to a list
    r_speed = cdf.varget('Bulk_Speed')
    for speed in r_speed:
        solar_wind_speed.append(speed)

    # combining all the parts of the date into one datetime object
    for j in range(0, len(raw_time)):
        year = raw_time[j][0]
        month = raw_time[j][1]
        day = raw_time[j][2]
        hour = raw_time[j][3]
        min = raw_time[j][4]
        sec = raw_time[j][5]
        ms = raw_time[j][6]
        time.append(datetime(year, month, day, hour, min, sec, ms))

    # combining the data into a dataframe
    data = pd.DataFrame()
    data['Date'] = time
    data['Solar_wind_speed'] = solar_wind_speed

    return data


def stereoa_obs_format(start_date, end_date, folder):

    """
    Function to combine the STEREO-A real time data into one file in the format that is accepted by BRaVDA.
    :param start_date: start date of the data window
    :param end_date: end date of the data window
    :param folder: where the folder containing the data is located and where the file will be saved
    :return:
    """

    # taking the date and finding the folder
    # turning the date into a string to search the folder
    folder_date = f.date_string(start_date)
    data_folder = os.path.join(folder, 'STEREO-A_raw', folder_date)

    # listing all the files in the folder
    try:
        files = os.listdir(data_folder)
    except:
        raise ValueError('Data does not exist for this date.')

    # empty lists to append the data to
    dates = list()
    solar_wind_speed = list()

    # looping through the files to extract the data
    for file in files:
        path = os.path.join(data_folder, file)
        data = stereoa_cdf_reader(path)
        for i in range(0, len(data)):
            dates.append(data['Date'].iloc[i])
            solar_wind_speed.append(data['Solar_wind_speed'].iloc[i])

    # dataframe containing all the data
    full_df = pd.DataFrame()
    full_df['Date'] = dates
    full_df['Solar_wind_speed'] = solar_wind_speed

    # removing the unphysical values and changing them to NaNs
    full_df['Solar_wind_speed'].loc[full_df['Solar_wind_speed'] < 0] = np.nan

    # list of hourly dates
    hourly_dates = f.date_list(start_date, end_date, timedelta(hours=1))
    # list for the hourly solar wind speed
    hourly_solar_wind_speed = []
    # lists for the year, day of year and hour
    years = list()
    doys = list()
    hours = list()

    # averaging the data to an hourly resolution
    for i in range(0, len(hourly_dates)):
        data_req = full_df.loc[(full_df['Date'] >= hourly_dates[i] - timedelta(minutes=30)) &
                               (full_df['Date'] < hourly_dates[i] + timedelta(minutes=30))]
        hourly_solar_wind_speed.append(data_req['Solar_wind_speed'].mean())
        years.append(f.date_to_doy(hourly_dates[i])[0])
        doys.append(f.date_to_doy(hourly_dates[i])[1])
        hours.append(f.date_to_doy(hourly_dates[i])[2])

    # putting the hourly data into a dataframe
    hourly_df = pd.DataFrame()
    hourly_df['Year'] = years
    hourly_df['DOY'] = doys
    hourly_df['Hour'] = hours
    hourly_df['Solar_wind_speed'] = hourly_solar_wind_speed

    # saves the formatted data as a text file in a specified directory
    # the file name is STEREO-A_rt_observations.txt so that it can be overwritten every time it is run
    np.savetxt(os.path.join(folder, str('STEREO-A_rt_observations.txt')), hourly_df.values,
               fmt=['%4.0d', '%4.0d', '%3.0d', '%6.0f'])

    # creating a file for STEREO-B - this is needed as BRaVDA needs three observation files to run, even if they are
    # not all used. As STEREO-B is no longer operational, this file contains only NaNs since 2014
    stb_speed = [np.nan] * len(hourly_df)
    stb_hourly_df = pd.DataFrame()
    stb_hourly_df['Year'] = years
    stb_hourly_df['DOY'] = doys
    stb_hourly_df['Hour'] = hours
    stb_hourly_df['Solar_wind_speed'] = stb_speed

    # saving it as a text file
    np.savetxt(os.path.join(folder, str('STEREO-B_rt_observations.txt')), stb_hourly_df.values,
               fmt=['%4.0d', '%4.0d', '%3.0d', '%6.0f'])


def stereoa_real_time_obs(start_date, end_date, directory):

    """
    Function to pull together the downloading of the STEREO-A data and formatting it to be used in BRaVDA.
    :param start_date: start date of the data to be downloaded
    :param end_date: end date of the data to be downloaded
    :param directory: where the data will be saved
    :return:
    """

    # starting a timer to see how long the code takes
    timer_start = timer()

    # downloading the STEREO-A observations
    stereoa_obs_download(start_date, end_date, directory)
    # formatting the observations
    stereoa_obs_format(start_date, end_date, directory)

    # stopping the timer and printing out the time it took for the code to run
    timer_end = timer()
    print(timer_end - timer_start, 'seconds to download and format STEREO-A data.')


if __name__ == '__main__':
    stereoa_real_time_obs(datetime(2023, 7, 15),
                          datetime(2023, 7, 20),
                          'D:\\PhD\\Real_time_data_download\\Data')


