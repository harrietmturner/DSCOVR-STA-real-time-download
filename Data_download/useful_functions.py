__author__ = 'Harriet Turner'
__email__ = 'h.turner3@pgr.reading.ac.uk'

import requests
from bs4 import BeautifulSoup as bs
from datetime import datetime, timedelta
from astropy.time import Time


def start_dates(start, end, delta):

    """
    Function to iterate between a start and end date with a specified gap between.
    :param start: start date, datetime object - datetime(YYYY,m,d,H,M,S)
    :param end: end date, datetime object - datetime(YYYY,m,d,H,M,S)
    :param delta: gap between the dates, timedelta objects - eg. timedelta(days=...)
    :return:
    """

    current = start
    while current <= end:
        yield current
        current += delta


def date_list(start, end, delta):

    """
    Function to return a list of dates with a given start, end and gap between.
    :param start: start date of the list, datetime object
    :param end: end date of the list, datetime object
    :param delta: gap between the dates
    :return: list of dates
    """

    dates = list()
    for result in start_dates(start, end, delta):
        dates.append(result)

    return dates


def dscovr_file_sort_key(name):

    """
    Function to split up the DSCOVR file name by underscores so that it can be sorted by the start date of the
    data file.
    :param name: file name
    :return: start date of the file
    """

    txt_split = name.split('_')
    return int(txt_split[3][1:])


def flatten_list(list_to_flatten):

    """
    Function to take a list of lists and turn it into a single list.
    :param list_to_flatten: the list to requiring flattening
    :return: the flat list
    """

    return [item for sublist in list_to_flatten for item in sublist]


def web_scraper(url, filename):

    """
    Function to download data from a URL and save it to a directory.
    :param url: the web address where the data are downloaded from
    :param filename: the file path for where the data is to be saved
    :return: data saved in file
    """

    # write the data from the URL to a file
    r = requests.get(url, stream='True')
    open(filename, 'wb').write(r.content)


def webpage_links(url):

    """
    Function to return all the links from a webpage.
    :param url: link to the webpage
    :return: list of links from the webpage
    """

    # gets all the links from the given URL
    req = requests.get(url)

    soup = bs(req.text, "html.parser")

    # appends all the links to a list
    links = list()
    for link in soup.find_all('a'):
        links.append(link.get('href'))

    return links


def rt_directory_finder(date, master_page):

    """
    Function to find the correct data directory for the given date.
    :param date: date of the data requires, datetime object
    :param master_page: url of the parent page that holds the data in year and month directories
    :return: url, str
    """

    # converts the date into a string and adds a zero in front of the month if it is < 10
    year_str = str(date.year)
    if date.month < 10:
        month_str = '0' + str(date.month)
    else:
        month_str = str(date.month)

    # returns the url for the given year and month
    return master_page + year_str + '/' + month_str


def date_to_doy(date):

    """
    Function to take a datetime object and turn it into a year, day of year and hour.
    :param date: date requiring changing, datetime
    :return: year, DOY and hour, int
    """

    # splitting the date into the components
    year = date.year
    # calculating the day of year
    doy = (date - datetime(date.year, 1, 1) + timedelta(days=1)).days
    hour = date.hour

    return year, doy, hour


def date_string(date):

    """
    Function to take a date and convert it into a string in the form YYYYMMDD.
    :param date: date requiring changing, datetime object
    :return: date, str
    """

    # splits the date up into its components to turn it into a string
    year_str = str(date.year)
    month = date.month
    day = date.day
    # putting a zero in front of months and/ or days if they are < 10
    if month < 10:
        month_str = str(0) + str(month)
    else:
        month_str = str(month)
    if day < 10:
        day_str = str(0) + str(day)
    else:
        day_str = str(day)

    # making the date string
    date_str = year_str + month_str + day_str

    return date_str


def date_to_mjd(date):

    """
    Function to transform a datetime object into MJD.
    :param date: date, datetime object
    :return: date in mjd
    """

    if type(date) != datetime:
        raise ValueError("Date needs to be a datetime object.")

    # creates an astropy time object from a datetime object
    t = Time(date, format='datetime')
    # converts the time to MJD format
    mjd_t = t.mjd

    return mjd_t
