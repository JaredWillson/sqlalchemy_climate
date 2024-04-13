# Import the dependencies.
import numpy as np
import pandas as pd
import datetime as dt
import sqlalchemy
from sqlalchemy import desc, func, create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite", echo=False)

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(autoload_with=engine)

# Save references to each table
measurement = Base.classes.measurement
station = Base.classes.station

#################################################
# Measurements Data Used in Multiple Routes
#################################################

# Some of our data will be used in multiple routes,
# so we will run the queries here and store them in variables
# where they will be usable by the routes without needing
# to repeat any of the code in the various routes. Specifically,
# we will want to collect temp and precip data for all stations
# for the last year of observations.

# Create our session (link) from Python to the DB
session = Session(engine)

# Find the most recent date in the data set.
last_data = dt.datetime.strptime(session.query(measurement).\
    order_by(measurement.date.desc()).\
    first().date, "%Y-%m-%d")

# Calculate the date one year from the last date in data set.
year_ago = last_data - dt.timedelta(days=366)

# Perform a query to retrieve the data and precipitation scores
full_year = session.query(measurement.id,
    measurement.station,
    measurement.date,
    measurement.prcp,
    measurement.tobs).\
    filter(measurement.date > year_ago).all()

# Save the query results as a Pandas DataFrame. Explicitly set the column names
columns = ['ID', 'Station', 'Date', 'Precipitation', 'Temperatures']
measurements_df = pd.DataFrame(data=full_year, columns=columns)
measurements_df.set_index('ID', drop=True, inplace=True)
measurements_df = measurements_df.dropna()

# Sort the dataframe by date
measurements_df = measurements_df.sort_values(by=['Date'], ascending=True)

# We will also need to use the 'most frequent' station in more than one route...
most_frequent = session.query(measurement.station).\
        group_by(measurement.station).\
        order_by(desc(func.count(measurement.station))).\
        first()

# Now we can close out our session.
session.close


#################################################
# Flask Setup
#################################################
from flask import Flask, jsonify

app = Flask(__name__)

#################################################
# Flask Routes
#################################################

# First the static home route that lists available pages
@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/2016-08-31<br>"
        f"/api/v1.0/2016-08-31/2017-01-31"
    )

# Next, the static precipitation route which lists all
# observations for the most recent year of data
@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)  
    
    # From our earlier collected measurements dataframe, we will
    # want to down select just the date and precipitation data
    precip_df = measurements_df[['Date','Precipitation']]
    
    # With our graphing in the Jupyter notebook earlier, we did not need to ensure
    # that the dataframe was unique by date since we were graphing each measurement
    # of precipitation, but that won't work for this use case since we will be
    # converting our data to a dictionary, so each date (index) will need to be unique
    # As a result, I will use group by and grab the average across the stations
    # for each unique date.
    precip_sum_df = pd.DataFrame(precip_df.groupby(['Date'])['Precipitation'].mean().round(2))


    # Now we need to convert our dataframe to a dictionary so we can easily
    # 'Jsonify' it...
    precip = precip_sum_df.T.to_dict('records') 
    # Close out our SQLite session...
    session.close
    # Return our result
    return jsonify(precip)

# Next comes the static list of stations in the form of
# a list of dictionaries
@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    
    # Now we will need a list of stations that we can jsonify...
    stations = session.query(station.station,
        station.name,
        station.latitude,
        station.longitude,
        station.elevation)
    sta_columns = ['Station', 'Name', 'Latitude', 'Longitude', 'Elevation']
    stations_df = pd.DataFrame(data=stations, columns=sta_columns)
    stations_dict = stations_df.to_dict('records')
    # Close out our SQLite session...
    session.close
    # Return our result as a jsonified list of dictionaries
    return jsonify(stations_dict)

@app.route("/api/v1.0/tobs")
def tobs():
    # Next, we need show one year of history from the most active weather station...
    # First we open our SQL session
    session = Session(engine)
        
    # First, we can select just the dataset that is related to our 'most frequent' station...
    df_filtered = measurements_df[measurements_df['Station'] == most_frequent[0]]

    # Now we need to convert that dataframe to a dictionary for "jsonification"
    # All we need is the date and temperatures data
    temp_df = df_filtered[['Date','Temperatures']]
    temp_df.set_index('Date', drop=True, inplace=True)
    temps = temp_df.T.to_dict('records')

    # Close out our SQLite session...
    session.close

    # Return our result...
    return jsonify(temps)

@app.route("/api/v1.0/<start>")
def user_start(start):
    start = start.replace("%20", "-")
    start = dt.datetime.strptime(start, "%Y-%m-%d")

    # We are going to use the start date above as our starting point
    # rather than collecting a year's woth of data. We will still want our
    # "most frequent" station as the source, so we can use that as a filter.

    # First we open our SQL session
    session = Session(engine)

    # Now we run our query to get our results...
    metrics = session.query(func.min(measurement.tobs),
        func.avg(measurement.tobs),
        func.max(measurement.tobs)).\
        filter(measurement.station==most_frequent[0],
         measurement.date >= start)

    columns = ['Min', 'Avg', 'Max']
    metrics_df = pd.DataFrame(data=metrics, columns=columns)

    result = metrics_df.T.to_dict()[0]

    # Close out our SQLite session...
    session.close
    return jsonify(result)

@app.route("/api/v1.0/<start>/<end>")
def user_start_end(start, end):
    start = start.replace("%20", "-")
    start = dt.datetime.strptime(start, "%Y-%m-%d")
    end = end.replace("%20", "-")
    end = dt.datetime.strptime(end, "%Y-%m-%d")

    # We are going to use the start date above as our starting point
    # and our end date above as our end date

    # First we open our SQL session
    session = Session(engine)

    # Now we run our query to get our results...
    metrics = session.query(func.min(measurement.tobs),
        func.avg(measurement.tobs),
        func.max(measurement.tobs)).\
        filter(measurement.station==most_frequent[0],
        measurement.date >= start,
        measurement.date <= end)

    columns = ['Min', 'Avg', 'Max']
    metrics_df = pd.DataFrame(data=metrics, columns=columns)

    result = metrics_df.T.to_dict()[0]

    # Close out our SQLite session...
    session.close
    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True)