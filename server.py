#  COVIDSCAPE server

import boto3
import requests
from flask import Flask, redirect, url_for, request
import csv
import codecs
from math import radians, cos, sin, asin, sqrt, atan2
from flask_cors import CORS, cross_origin  # This is the magic


# global dict to store all users in memory
user_dict = {}
# initialize Flask app for incoming HTTP POST msgs
app = Flask(__name__)
CORS(app)  # This makes the CORS feature cover all routes in the app

# client to handle s3 files. Example here using Shuo's AWS keys
aws_access_key_id = 'AKIAJMM6XATNHG4XWZ7Q'
aws_secret_access_key = 'ojmaEac1shEX9+oERDs/Ti40Yb1tK9M2Ux90gLct'
region_name = "us-west-1"

s3_client = boto3.client("s3",
                         aws_access_key_id=aws_access_key_id,
                         aws_secret_access_key=aws_secret_access_key,
                         region_name=region_name)


# class to save Client data
class Client:
    def __init__(self, userid, email, phone):
        self.userid = userid
        self.email = email
        self.phone = phone
        self.diagnosed = False
        self.diagnosed_time = None
        self._subscribe_to_ses()

    def diagnose(self, diagnosed_timestamp):
        self.diagnosed = True
        self.diagnosed_time = diagnosed_timestamp

    def _subscribe_to_ses(self):
        global aws_access_key_id, aws_secret_access_key, region_name
        ses = boto3.client('ses',
                           aws_access_key_id=aws_access_key_id,
                           aws_secret_access_key=aws_secret_access_key,
                           region_name=region_name)
        response = ses.verify_email_identity(
            EmailAddress=self.email
        )


# Calculation of the total risk score
def calculate_total_risk_score(ml_score, social_distancing, mask, indoor):
    """
    Calculate and return the total risk score
    Args:
        ml_score (float): the risk score from ML algorithm. Float value from 0 to 1
        social_distancing (bool): indicates whether social distancing is performed
        mask (bool): indicates whether mask is worn
        indoor (bool): indicates whether user is indoors or not

    Returns:
        float: normalized total risk score from 0 to 1 where 1 indicates maximum risk

    """
    static_risk_multiplier_dict = {
        'no_social_distancing': 4,  # study shows chance reduce to 1/4
        # 'public_transportation': 4.3,  # study shows chance increase by 4.3
        # 'place_of_worship': 16,  # study shows chance increase by 16x
        'no_mask': 2.86,  # study shows chance reduce by 65%
        'outdoor': 18.6  # study shows indoor is 18.6 times more dangerous
    }
    # Calculate max to normalize score later
    max_score = 1
    for key in static_risk_multiplier_dict:
        max_score *= static_risk_multiplier_dict[key]

    # Calculate score
    score = ml_score
    if not social_distancing:
        score *= static_risk_multiplier_dict['no_social_distancing']
    if not mask:
        score *= static_risk_multiplier_dict['no_mask']
    if indoor:
        score *= static_risk_multiplier_dict['outdoor']

    # Normalize score to be 0 to 1
    score /= max_score
    return score


# Amazon SNS vs raw code
# SNS using boto3
def send_sns(user_list):
    """
    Send SNS publish msgs to phone number and email
    Alternatively use Amazon SES for email

    """
    # Create an SNS client
    global aws_access_key_id, aws_secret_access_key, region_name
    sns = boto3.client(
        "sns",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )
    # Create an SES client
    ses = boto3.client('ses',
                       aws_access_key_id=aws_access_key_id,
                       aws_secret_access_key=aws_secret_access_key,
                       region_name=region_name)
    # Settings for SES
    SENDER = "COVIDSCAPE <polarbear51009@gmail.com>"  # Need to change to actual verifiable email (maybe we can register covidscape@gmail.com)
    SUBJECT = "COVIDSCAPE Notification - Possible Contact with Diagnosed Client"
    BODY_TEXT = (f"""\
                    COVIDSCAPE Email Notification 

                    You may have been in contact with a diagnosed client in the past 14 days.
                """
                 )
    # The HTML body of the email.
    BODY_HTML = """<html>
    <head></head>
    <body>
      <h1>COVIDSCAPE Email Notification</h1>
      <p>You may have been in contact with a diagnosed client in the past 14 days.</p>
    </body>
    </html>
                """
    # The character encoding for the email.
    CHARSET = "UTF-8"

    # Subscribe user to SNS topic
    # topic_arn = 'arn:aws:sns:region:0123456789:my-topic-arn'
    topic_response = sns.create_topic(Name=f'backtrace_covidscape')
    topic_arn = topic_response['TopicArn']
    for user in user_list:
        # Create SMS subscription
        response = sns.subscribe(TopicArn=topic_arn, Protocol="SMS", Endpoint=user.phone)
        # subscription_arn = response["SubscriptionArn"]

        # Create email subscription
        # response = sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint=user.email)
        # subscription_arn = response["SubscriptionArn"]

        # Send email with SES
        # Replace recipient@example.com with a "To" address. If your account
        # is still in the sandbox, this address must be verified.
        RECIPIENT = user.email
        response = ses.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': BODY_HTML,
                    },
                    'Text': {
                        'Charset': CHARSET,
                        'Data': BODY_TEXT,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
        )

        # Send SMS msg to phone
        sns.publish(
            PhoneNumber=user.phone,
            Message=f"COVIDSCAPE SMS Notification \n\n"
                    f"You may have been in contact with a diagnosed client in the past 14 days."
        )

    # Publish message to the specified SNS topic to be sent to emails
    # response = sns.publish(
    #     TopicArn=topic_arn,
    #     Subject=f"COVIDSCAPE Notification - Possible Contact with Diagnosed Client",
    #     Message=f"""\
    #                 COVIDSCAPE Email Notification
    #
    #                 You may have been in contact with a diagnosed client in the past 14 days.
    #             """
    # )
    # Print out the response
    # print(response)
    # Delete topic when we are done
    sns.delete_topic(TopicArn=topic_arn)


def backtrace_and_notify(source_user):
    affected_user_list = []
    user_history = get_s3_data()
    source_history = user_history.get(source_user)
    if not source_history:
        print(f'No history found for source user: {source_user}')
        return
    # remove source user from dictionary
    user_history.pop(source_user)
    # Now compare and find possible contacts...
    # Need to determine tolerances for GPS location and timestamps. But for exact match:
    global user_dict
    # Example tolerance
    lat_tol = 0.0001  # 0.0001 is +/- 5.55m.
    lon_tol = 0.0001
    time_tol = 1800000  # timestamp is in milliseconds - this is +/- 1 hour
    fourteen_days_in_msec = 1209600000  # Number of milliseconds for 14 days
    fourteen_days_from_diagnosed = float(user_dict[source_user].diagnosed_time) - fourteen_days_in_msec
    for source_entry in source_history:
        if float(source_entry['timestamp']) >= fourteen_days_from_diagnosed:
            for other_user in user_history:
                for other_entry in user_history[other_user]:
                    if (other_entry['lat']-lat_tol <= source_entry['lat'] <= other_entry['lat']+lat_tol) and \
                            (other_entry['lon']-lon_tol <= source_entry['lon'] <= other_entry['lon']+lon_tol) and \
                            (other_entry['timestamp']-time_tol <= source_entry['timestamp'] <= other_entry['timestamp']+time_tol):
                        # Match found within tolerance - add to list to be notified and try next user
                        affected_user_list.append(user_dict[other_user])
                        break

    send_sns(user_list=affected_user_list)  # send SNS to affected users


def get_s3_data(raw_data=False, data_type='node-red'):
    """
    Gets the data from S3 and returns raw stream or parsed user history

    Args:
        raw_data: True indicates return raw file stream. False indicates parse for user history. Defaults to False.
        data_type: data type to get from s3
    Returns:
        raw file stream object if raw_data = True
        user history map dictionary of list of dictionaries if raw_data is False
    """
    data = dict()
    if data_type == 'node-red':
        data = s3_client.get_object(Bucket='covidscapeshuo', Key='user-log_dat.csv')
    elif data_type == 'city_id':
        data = s3_client.get_object(Bucket='covidscapeshuo', Key='city.csv')
    elif data_type == 'ml_score':
        data = s3_client.get_object(Bucket='covidscapeshuo', Key='ml_score.csv')

    if raw_data:
        return data['Body']  # This returns the whole raw file stream

    # Parse input to generate user history map
    user_history = dict()
    for row in csv.DictReader(codecs.getreader('utf-8')(data['Body'])):
        # Extract and use the actual user id
        userid = row['userid']
        if '/' in row['userid']:
            userid = row['userid'].split('/')[1]
        if row['userid'] not in user_history.keys():
            user_history[userid] = []
        entry_dict = {
            'lat': float(row['lat']),
            'lon': float(row['lon']),
            'timestamp': float(row['timestamp']),
        }
        user_history[userid].append(entry_dict)
    return user_history


# Calculate closest city
def haversine(lat1, lon1, lat2, lon2):
    # r = 3959.87433 # this is in miles.  For Earth radius in kilometers use 6372.8 km
    # dLat = radians(lat2 - lat1)
    # dLon = radians(lon2 - lon1)
    # lat1 = radians(lat1)
    # lat2 = radians(lat2)
    # a = sin(dLat/2)**2 + cos(lat1)*cos(lat2)*sin(dLon/2)**2
    # c = 2*asin(sqrt(a))
    # return r * c
    radius = 6371  # km

    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) * sin(dlat / 2) + cos(radians(lat1)) \
        * cos(radians(lat2)) * sin(dlon / 2) * sin(dlon / 2)
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return radius * c


def find_closest_city_id(user_lat, user_lon):
    closest_city_id = None
    closest_city_distance = 99999999
    # Get city data from s3 bucket
    s3_city_data = get_s3_data(raw_data=True, data_type='city_id')
    city_csv_dict = csv.DictReader(codecs.getreader('utf-8')(s3_city_data))
    # Find closest city id by distance
    for city in list(city_csv_dict):
        distance = haversine(user_lat, user_lon, float(city['lat']), float(city['lon']))
        print(f"cityid: {city.get('id')}'s distance is {distance}")
        print(f"user lat: {user_lat}, user_lon: {user_lon}. city_lat: {float(city['lat'])}, city lon: {float(city['lon'])}")
        if distance < closest_city_distance:
            closest_city_id = city.get('id')
            closest_city_distance = distance
    return closest_city_id


def get_ml_score(user_lat, user_lon, days):
    closest_city_id = find_closest_city_id(user_lat, user_lon)
    if closest_city_id is not None:
        print(f'city id: {closest_city_id}')
        s3_ml_data = get_s3_data(raw_data=True, data_type='ml_score')
        ml_csv_dict = csv.DictReader(codecs.getreader('utf-8')(s3_ml_data))
        for row in ml_csv_dict:
            if row.get('id') == closest_city_id:
                return float(row[days])
    return 0.5  # return 0.5 if city is not found


# Server main function
@app.route('/', methods=['GET', 'POST'])
@cross_origin()
def main():
    """
    Main COVIDSCAPE server runtime.

    Expected client request data is a dictionary containing the following:
        register (bool): whether its a new client that is registering account
        diagnosed (bool): whether this request is client reporting contracting virus
        userid (str): unique user id
        email (str): user email
        phone (str): user phone
        node_red_location (bool): True means use location/time from node red. False means use the location and time from client
        lat (float): gps latitude (used if node_red_location is False)
        lon (float):  gps longitude (used if node_red_location is False)
        timestamp (float? datetime format?): time to go to above location (used if node_red_location is False)
        social_distancing (bool): True means strict social distancing is practiced
        mask (bool): True means mask on
        indoor (bool): True means indoor, False means outdoor
        endpoint (str): the client app's endpoint to send HTTP POST response to
        days (int): number of days into the future (0 - 3)
    """
    global user_dict
    # Get requests from client
    while True:  # Ideally this server loop will never stop
        # Get user request data
        if request.method == 'POST':
            data = request.form
        else:
            data = request.args
        # Register new client
        if data.get('register') == 'True':
            global user_dict
            if data['userid'] not in user_dict.keys():  # only register if not already registered
                new_client = Client(data['userid'], data['email'], data['phone'])
                user_dict[data['userid']] = new_client
                return f"Successfully processed User Registration for {data['userid']}"
            return f"User: {data['userid']} is already registered."

        # User notifying contracted virus
        elif data.get('diagnosed') == 'True':  # if diagnosed - do notification
            user_dict[data['userid']].diagnose(data['timestamp'])
            backtrace_and_notify(source_user=data['userid'])
            return f"Successfully processed input from {data['userid']} and sent notifications to other users"
        # The general request
        else:
            user_lat = data.get('lat')
            user_lon = data.get('lon')
            if data.get('node_red_location') == 'True':
                # Get User GPS from S3 file
                s3_data = get_s3_data(raw_data=True)
                # Search from the bottom using reversed so we get latest entry
                user_data_found = False

                csv_dict_reader = csv.DictReader(codecs.getreader('utf-8')(s3_data))
                for reversed_row in reversed(list(csv_dict_reader)):
                    userid = reversed_row['userid']
                    if '/' in reversed_row['userid']:
                        userid = reversed_row['userid'].split('/')[1]
                    # extract latest data for this user
                    if userid == data['userid']:
                        user_lat = reversed_row['lat']
                        user_lon = reversed_row['lon']
                        user_data_found = True
                        break
                if not user_data_found:
                    print(f"data[userid] = {data['userid']}")
                    raise Exception('User data not found from S3 csv file')
            # Get ml score from S3
            ml_risk_score = get_ml_score(float(user_lat), float(user_lon), data['days'])
            # Calculate total risk score
            total_risk_score = calculate_total_risk_score(ml_risk_score, data['social_distancing'] == 'True',
                                                          data['mask'] == 'True', data['indoor'] == 'True')

            # Send response to client
            # requests.post(url=data['endpoint'], data={'total_risk_score': total_risk_score})

            # If this user is already diagnosed - need to notify others that are there
            # if user_dict[data['userid']].diagnosed:
            #     backtrace_and_notify(source_user=data['userid'])

            return f"total_risk_score: {total_risk_score}"


if __name__ == "__main__":
    # main()
    app.run(debug=False, host='0.0.0.0')


"""
Example usages:

Register a Client:
http://52.53.216.170:5000/?userid=tester&register=True&email=user@gmail.com&phone=16261234567

Reporting Diagnosed Client:
http://52.53.216.170:5000/?userid=tester&diagnosed=True&timestamp=1623445645000

Request Risk Score for Client:
http://52.53.216.170:5000/?userid=tester&lat=33.8690197&lon=-118.0796195&days=1&social_distancing=True&mask=False&indoor=True

Request Risk Score for Client but use latest Node red location:
http://52.53.216.170:5000/?userid=tester&node_red_location=True&days=1&social_distancing=True&mask=False&indoor=True
"""