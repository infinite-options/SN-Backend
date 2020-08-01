# -*- coding: utf-8 -*-
# @Author: Japan Parikh
# @Date:   2019-02-16 15:26:12
# @Last Modified by:   Howard Ng	
# @Last Modified time: 2020-05-15 20:00:00


import os
import uuid
import boto3
import json
import math
from datetime import datetime
from datetime import timedelta
from pytz import timezone
import random
import string
import stripe

from flask import Flask, request, render_template
from flask_restful import Resource, Api
from flask_cors import CORS
from flask_mail import Mail, Message

from werkzeug.exceptions import BadRequest, NotFound
from werkzeug.security import generate_password_hash, \
     check_password_hash

from NotificationHub import Notification
from NotificationHub import NotificationHub
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from twilio.rest import Client

app = Flask(__name__, template_folder='assets')
cors = CORS(app, resources={r'/api/*': {'origins': '*'}})

app.config['MAIL_USERNAME'] = os.environ.get('EMAIL')
app.config['MAIL_PASSWORD'] = os.environ.get('PASSWORD')
# app.config['MAIL_USERNAME'] = ''
# app.config['MAIL_PASSWORD'] = ''

# Setting for mydomain.com
app.config['MAIL_SERVER'] = 'smtp.mydomain.com'
app.config['MAIL_PORT'] = 465

# Setting for gmail
# app.config['MAIL_SERVER'] = 'smtp.gmail.com'
# app.config['MAIL_PORT'] = 465

app.config['MAIL_USE_TLS'] = False
app.config['MAIL_USE_SSL'] = True
app.config['DEBUG'] = True
#app.config['DEBUG'] = False

app.config['STRIPE_SECRET_KEY'] = os.environ.get('STRIPE_SECRET_KEY')

mail = Mail(app)
api = Api(app)


db = boto3.client('dynamodb', region_name="us-west-1")
s3 = boto3.client('s3')


# aws s3 bucket where the image is stored
BUCKET_NAME = os.environ.get('MEAL_IMAGES_BUCKET')
#BUCKET_NAME = 'servingnow'
# allowed extensions for uploading a profile photo file
ALLOWED_EXTENSIONS = set(['png', 'jpg', 'jpeg'])

# For Push notification
isDebug = False
NOTIFICATION_HUB_KEY = os.environ.get('NOTIFICATION_HUB_KEY')
NOTIFICATION_HUB_NAME = os.environ.get('NOTIFICATION_HUB_NAME')

TWILIO_ACCOUNT_SID = os.environ.get('TWILIO_ACCOUNT_SID')
TWILIO_AUTH_TOKEN = os.environ.get('TWILIO_AUTH_TOKEN')
#sms_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

def helper_upload_meal_img(file, bucket, key):
    if file and allowed_file(file.filename):
        filename = 'https://s3-us-west-1.amazonaws.com/' \
                   + str(bucket) + '/' + str(key)
       
        upload_file = s3.put_object(
                            Bucket=bucket,
                            Body=file,
                            Key=key,
                            ACL='public-read',
                            ContentType='image/jpeg'
                        )
        return filename
    return None

def helper_upload_refund_img(file, bucket, key):
    if file:
        filename = 'https://s3-us-west-1.amazonaws.com/' \
                   + str(bucket) + '/' + str(key)
        #print('bucket:{}'.format(bucket))
        upload_file = s3.put_object(
                            Bucket=bucket,
                            Body=file,
                            Key=key,
                            ACL='public-read',
                            ContentType='image/png'
                        )
        return filename
    return None

def allowed_file(filename):
    """Checks if the file is allowed to upload"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# ===========================================================


def kitchenExists(kitchen_id):
    # scan to check if the kitchen name exists
    kitchen = db.scan(TableName='kitchens',
        FilterExpression='kitchen_id = :val',
        ExpressionAttributeValues={
            ':val': {'S': kitchen_id}
        }
    )

    return not kitchen.get('Items') == []

def couponExists(coupon_id):
    # scan to check if the kitchen name exists
    coupon = db.scan(TableName='coupons',
        FilterExpression='coupon_id = :val',
        ExpressionAttributeValues={
            ':val': {'S': coupon_id}
        }
    )

    return not coupon.get('Items') == []


# ===========================================================

class MealOrders(Resource):
    def post(self):
        """Collects the information of the order
           and stores it to the database.
        """
        response = {}
        data = request.get_json(force=True)
        created_at = datetime.now(tz=timezone('US/Pacific')).strftime("%Y-%m-%dT%H:%M:%S")

        if data.get('email') == None:
            raise BadRequest('Request failed. Please provide email')
        if data.get('name') == None:
            raise BadRequest('Request failed. Please provide name')
        if data.get('street') == None:
            raise BadRequest('Request failed. Please provide street')
        if data.get('zipCode') == None:
            raise BadRequest('Request failed. Please provide zipCode')
        if data.get('city') == None:
            raise BadRequest('Request failed. Please provide city')
        if data.get('state') == None:
            raise BadRequest('Request failed. Please provide state')
        if data.get('totalAmount') == None:
            raise BadRequest('Request failed. Please provide totalAmount')
        if data.get('paid') == None:
            raise BadRequest('Request failed. Please provide paid')
        if data.get('paymentType') == None:
            raise BadRequest('Request failed. Please provide paymentType')
        if data.get('ordered_items') == None:
            raise BadRequest('Request failed. Please provide ordered_items')
        if data.get('phone') == None:
            raise BadRequest('Request failed. Please provide phone')
        if data.get('kitchen_id') == None:
            raise BadRequest('Request failed. Please provide kitchen_id')
        if data.get('delivery_instructions') == None:
            data['delivery_instructions'] = ''
        if data.get('address_unit') == None:
            data['address_unit'] = ''
        if data.get('notification_enabled') == None:
            data['notification_enabled'] = False
        if data.get('addressLongitude') == None:
            data['addressLongitude'] = '0'
        if data.get('addressLatitude') == None:
            data['addressLatitude'] = '0'
        if data.get('appVersion') == None:
            data['appVersion'] = '<=1.6'

        kitchenFound = kitchenExists(data['kitchen_id'])

        # raise exception if the kitchen does not exists
        if not kitchenFound:
            raise BadRequest('kitchen does not exist')

        order_id = data['order_id']
        totalAmount = data['totalAmount']

        order_details = []

        for i in data['ordered_items']:
            product = db.scan(TableName='meals',
                FilterExpression='meal_id = :val',
                ProjectionExpression='meal_name, price',
                ExpressionAttributeValues={
                    ':val': {'S': i['meal_id']}
                })
            item = {}
            item['meal_id'] = {}
            item['meal_id']['S'] = i['meal_id']
            item['meal_name'] = {}
            item['meal_name']['S'] = product['Items'][0]['meal_name']['S']
            item['qty'] = {}
            item['qty']['N'] = str(i['qty'])
            item['price'] = {}
            item['price']['N'] = product['Items'][0]['price']['S']
            if (item['qty']['N'] != '0'):
                order_details.append(item)

        order_items = [{"M": x} for x in order_details]
        
        try:
            add_order = db.put_item(TableName='meal_orders',
                Item={'order_id': {'S': order_id},
                      'created_at': {'S': created_at},
                      'email': {'S': data['email']},
                      'name': {'S': data['name']},
                      'street': {'S': data['street']},
                      'zipCode': {'N': str(data['zipCode'])},
                      'city': {'S': data['city']},
                      'state': {'S': data['state']},
                      'totalAmount': {'N': str(totalAmount)},
                      'paid': {'BOOL': data['paid']},
                      'status': {'S': 'open'},
                      'paymentType': {'S': data['paymentType']},
                      'order_items':{'L': order_items},
                      'phone': {'S': str(data['phone'])},
                      'delivery_instructions' : {'S': data['delivery_instructions']},
                      'address_unit' : {'S': data['address_unit']},
                      'kitchen_id': {'S': str(data['kitchen_id'])},
                      'notification_enabled': {'BOOL': data['notification_enabled']},
                      'addressLongitude' : {'S': data['addressLongitude']},
                      'addressLatitude' : {'S': data['addressLatitude']},
                      'appVersion' : {'S': data['appVersion']}
                }
            )

            kitchen = db.get_item(TableName='kitchens',
                Key={'kitchen_id': {'S': data['kitchen_id']}},
                ProjectionExpression='kitchen_name, street, city, \
                    st, phone_number, pickup_time, first_name, kitchen_id, email'
            )
            
            customerMsg = Message(subject='Serving Now: Order Confirmation',
                            sender=app.config['MAIL_USERNAME'],
                            html=render_template('emailTemplate.html',
                            order_items=order_details,
                            kitchen=kitchen['Item'],
                            totalAmount=totalAmount,
                            name=data['name']),
                            recipients=[data['email'],"orders@servingnow.me"])
        
            prashantMsg = Message(subject='SN Admin: Order Confirmation',
                            sender=app.config['MAIL_USERNAME'],
                            html=render_template('emailTemplate.html',
                            order_items=order_details,
                            kitchen=kitchen['Item'],
                            totalAmount=totalAmount,
                            name=data['name']),
                            recipients=["pmarathay@gmail.com"])
        
            BusinessMsg = Message(subject='Farm Order Confirmation',
                          sender=app.config['MAIL_USERNAME'],
                          html=render_template('businessEmailTemplate.html',
                          order_items=order_details,
                          kitchen=kitchen['Item'],
                          totalAmount=totalAmount,
                          customer=data['name']),
                          recipients=[kitchen['Item']['email']['S'],"support@servingnow.me"] )

            mail.send(customerMsg)
            mail.send(prashantMsg)
            mail.send(BusinessMsg)

            response['message'] = 'Request successful'
            return response, 200
        except Exception as e:
            raise BadRequest('Request failed: ' + str(e))

    def get(self):
        """RETURNS ALL ORDERS PLACED TODAY"""
        response = {}
        todays_date = datetime.now(tz=timezone('US/Pacific')).strftime("%Y-%m-%d")

        try:
            orders = db.scan(TableName='meal_orders',
                FilterExpression='(contains(created_at, :x1))',
                ExpressionAttributeValues={
                    ':x1': {'S': todays_date}
                }
            )

            response['result'] = orders['Items']
            response['message'] = 'Request successful'
            return response, 200
        except:
            raise BadRequest('Request failed. please try again later.')


class RegisterKitchen(Resource):
    def post(self):
        response = {}
        data = request.get_json(force=True)
        created_at = datetime.now(tz=timezone('US/Pacific')).strftime("%Y-%m-%dT%H:%M:%S")

        if data.get('kitchen_name') == None \
          or data.get('description') == None \
          or data.get('email') == None \
          or data.get('username') == None \
          or data.get('password') == None \
          or data.get('first_name') == None \
          or data.get('last_name') == None \
          or data.get('street') == None \
          or data.get('city') == None \
          or data.get('st') == None \
          or data.get('zipcode') == None \
          or data.get('phone_number') == None \
          or data.get('close_time') == None \
          or data.get('open_time') == None \
          or data.get('delivery_open_time') == None \
          or data.get('delivery_close_time') == None \
          or data.get('pickup') == None \
          or data.get('delivery') == None \
          or data.get('reusable') == None \
          or data.get('disposable') == None \
          or data.get('can_cancel') == None:
            raise BadRequest('Request failed. Please provide all \
                              required information.')

        # scan to check if the kitchen name exists
        kitchen = db.scan(TableName="kitchens",
            FilterExpression='#name = :val',
            ExpressionAttributeNames={
                '#name': 'name'
            },
            ExpressionAttributeValues={
                ':val': {'S': data['name']}
            }
        )

        # raise exception if the kitchen name already exists
        if kitchen.get('Items') != []:
            response['message'] = 'This kitchen name is already taken.'
            return response, 400

        kitchen_id = uuid.uuid4().hex

        can_cancel = False
        if data['can_cancel'] == 'true':
          can_cancel = True

        try:
            add_kitchen = db.put_item(TableName='kitchens',
                Item={'kitchen_id': {'S': kitchen_id},
                      'created_at': {'S': created_at},
                      'kitchen_name': {'S': data['kitchen_name']},
                      'description': {'S': data['description']},
                      'username': {'S': data['username']},
                      'password': {'S': generate_password_hash(data['password'])},
                      'first_name': {'S': data['first_name']},
                      'last_name': {'S': data['last_name']},
                      'street': {'S': data['street']},
                      'city': {'S': data['city']},
                      'st': {'S': data['st']},
                      'zipcode': {'N': str(data['zipcode'])},
                      'phone_number': {'S': str(data['phone_number'])},
                      'open_time': {'S': str(data['open_time'])},
                      'close_time': {'S': str(data['close_time'])},
                      'isOpen': {'BOOL': False},
                      'email': {'S': data['email']},
                      'delivery_open_time': { 'S': data['delivery_open_time' ]},
                      'delivery_close_time': { 'S': data['delivery_close_time' ]},
                      'pickup': { 'BOOL': data['pickup']},
                      'delivery': { 'BOOL': data['delivery']},
                      'reusable': { 'BOOL': data['reusable']},
                      'disposable': { 'BOOL': data['disposable']},
                      'can_cancel': { 'BOOL': can_cancel}
                }
            )

            response['message'] = 'Request successful'
            response['kitchen_id'] = kitchen_id
            return response, 200
        except:
            raise BadRequest('Request failed. Please try again later.')


def formateTime(time):
    hours = time.rsplit(':', 1)[0]
    mins = time.rsplit(':', 1)[1]
    if hours == '00':
        return '{}:{} AM'.format('12', mins)
    elif hours >= '12' and hours < '24':
        if hours == '12':
            return '{}:{} PM'.format(hours, mins)
        return '{}:{} PM'.format((int(hours) - 12), mins)
    else:
        return '{}:{} AM'.format(hours, mins)

class Kitchens(Resource):
    def get(self):
        """Returns all kitchens"""
        response = {}

        try:
            kitchens = db.scan(TableName='kitchens',
                ProjectionExpression='kitchen_name, kitchen_id, \
                    close_time, description, open_time, isOpen, \
                    accepting_hours, is_accepting_24hr, delivery_hours, \
                    zipcode, available_zipcode',
            )

            result = []

            for kitchen in kitchens['Items']:
                kitchen['open_time']['S'] = formateTime(kitchen['open_time']['S'])
                kitchen['close_time']['S'] = formateTime(kitchen['close_time']['S'])

                if kitchen['isOpen']['BOOL'] == True:
                    result.insert(0, kitchen)
                else:
                    result.append(kitchen)

            response['message'] = 'Request successful'
            response['result'] = result
            return response, 200
        except:
            raise BadRequest('Request failed. Please try again later.')

class Coupons(Resource):
    bool_fields=['active','recurring']
    num_fields = ['credit','days','lim','num_used','coupon_type']
    @staticmethod
    def check_N_or_S(fi_eld):
        if 'N' in fi_eld.keys():
            if float(fi_eld['N'])>int(float(fi_eld['N'])):
                return float(fi_eld['N'])
            else:
                return int(fi_eld['N'])
        else:
            return fi_eld['S']

    def conv_str_values(self,body):
        for key in body.keys():
            if key in self.bool_fields:
                body[key] = body[key]=="true"
            elif key in self.num_fields:
                body[key] = float(body[key])
            else:
                continue
        return body
        
    def get(self):
        """Returns all kitchens"""
        response = {}
        pe = "coupon_id, active, credit, days, lim, notes, recurring, num_used, email_id, coupon_type, date_expired"
        try:
            coupons = db.scan(TableName='coupons',
                ProjectionExpression=pe
            )

            result = []
            # print(coupons['Items'])
            for coupon in coupons['Items']:
                my_coupon={}
                my_coupon['credit'] = self.check_N_or_S(coupon['credit'])
                my_coupon['days'] = self.check_N_or_S(coupon['days'])
                my_coupon['notes'] = self.check_N_or_S(coupon['notes'])
                my_coupon['coupon_id'] = self.check_N_or_S(coupon['coupon_id'])
                my_coupon['recurring'] = coupon['recurring']['BOOL']
                my_coupon['lim'] = self.check_N_or_S(coupon['lim'])
                my_coupon['num_used'] = self.check_N_or_S(coupon['num_used'])
                my_coupon['active'] = coupon['active']['BOOL']
                my_coupon['coupon_type'] = self.check_N_or_S(coupon['coupon_type'])
                my_coupon['date_expired'] = self.check_N_or_S(coupon['date_expired'])
                if 'email_id' in coupon:
                    my_coupon['email_id'] = self.check_N_or_S(coupon['email_id'])
                result.append(my_coupon)

            response['message'] = 'Request successful'
            response['result'] = result
            return response, 200
        except:
            raise BadRequest('Request failed. Please try again later.')


    def post(self):
        response = {}
        # body = request.get_json(force=True)
        
        try:
            body = request.form.to_dict()
            body = self.conv_str_values(body)
        except:
            body = request.get_json(force=True)
        if body.get('credit') == None \
          or body.get('active') == None \
          or body.get('days') == None \
          or body.get('notes') == None \
          or body.get('num_used') == None \
          or body.get('lim') == None \
          or body.get('coupon_type') == None:  
            raise BadRequest('Request failed. Please provide required details.')
        

        if body['lim']>1:
            body['recurring'] = True
        else:
            body['recurring'] = False
        # email_av = True
        while True:
            coupon_id = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(6))
            if couponExists(coupon_id):
                continue
            else:
                break
        
        exp_date = (datetime.now(tz=timezone('US/Pacific'))+timedelta(days=body['days'])).strftime("%Y-%m-%d")

        try:
            if body.get('email_id') == None:
                add_coupon = db.put_item(TableName='coupons',
                    Item={'coupon_id': {'S': coupon_id},
                            'credit': {'N': str(body['credit'])},
                            'active': {'BOOL': body['active']},
                            'days': {'N': str(body['days'])},
                            'notes': {'S': body['notes']},
                            'lim': {'N': str(body['lim'])},
                            'recurring': {'BOOL': body['recurring']},
                            'num_used': {'N': str(0)},
                            'date_expired': {'S': exp_date},
                            'coupon_type': {'N': str(body['coupon_type'])}
                    }
                )
            else:
                add_coupon = db.put_item(TableName='coupons',
                    Item={'coupon_id': {'S': coupon_id},
                            'credit': {'N': str(body['credit'])},
                            'active': {'BOOL': body['active']},
                            'days': {'N': str(body['days'])},
                            'notes': {'S': body['notes']},
                            'lim': {'N': str(body['lim'])},
                            'recurring': {'BOOL': body['recurring']},
                            'num_used': {'N': str(0)},
                            'date_expired': {'S': exp_date},
                            'coupon_type': {'N': str(body['coupon_type'])},
                            'email_id': {'S': body['email_id']}
                    }
                )

            response['message'] = f'Request successful with coupon id {coupon_id}'
            return response, 201
        except:
            raise BadRequest('Request failed. Please try again later.')

class Coupon(Resource):
    def get(self, coupon_id):
        coupon = db.scan(TableName='coupons',
            FilterExpression='coupon_id = :val',
            ExpressionAttributeValues={
                ':val': {'S': coupon_id}
            }
        )
        if (coupon.get('Items') == []):
            return "Coupon not found.", 404
        return coupon, 200
    
    def put(self, coupon_id):
    
        response = {}
        # data = request.get_json(force=True)

        if not couponExists(coupon_id):
            return "Coupon not found.", 404

        product = db.scan(TableName='coupons',
            FilterExpression='coupon_id = :val',
            ProjectionExpression='lim, num_used, date_expired, active',
            ExpressionAttributeValues={
                ':val': {'S': coupon_id}
            })
        lim1 =product["Items"][0]["lim"]["N"]
        num_used1 =product["Items"][0]["num_used"]["N"]
        exp_date = datetime.strptime(product["Items"][0]["date_expired"]["S"], '%Y-%m-%d')
        todays_date = datetime.now()
        delta  = exp_date - todays_date

        if int(lim1)==0:
            return "Coupon depleted"

        if delta.days<0 and product["Items"][0]["active"]["BOOL"]:
            update_coupon = db.update_item(TableName='coupons',
                Key={'coupon_id': {'S': str(coupon_id)}},
                UpdateExpression='SET active = :ac',
                ExpressionAttributeValues={
                    ':ac': {'BOOL': False}
                }
            )
            return "Coupon depleted"

        try:  
            # print(product)
            actie = True
            if int(lim1)-1==0:
                actie = not actie
            update_coupon = db.update_item(TableName='coupons',
                Key={'coupon_id': {'S': str(coupon_id)}},
                UpdateExpression='SET lim = :l, num_used = :nu, active = :ac',
                ExpressionAttributeValues={
                    ':l': {'N': str(int(lim1)-1)},
                    ':nu': {'N': str(int(num_used1)+1)},
                    ':ac': {'BOOL': actie}
                }
            )
            response['message'] = 'Request successful'
            return response, 201
        except:
            raise BadRequest('Request failed. Please try again later.')

class ZipCodes(Resource):
    def get(self):
        """Returns all Zipcodes"""
        response = {}
        try:
            lis_zip_codes =["94024",
                    "94087",
                    "95014",
                    "95030",
                    "95032",
                    "95051",
                    "95070",
                    "95111",
                    "95112",
                    "95120",
                    "95123",
                    "95124",
                    "95125",
                    "95129",
                    "95130",
                    "95128",
                    "95122",
                    "95118",
                    "95126",
                    "95136",
                    "95113",
                    "95117"]
                    #"94024,94087,95014,95030,95032,95051,95070,95111,95112,95120,95123,95124,95125,95129,95130,95128,95122,95118,95126,95136,95113,95117"
            result = {}
            result['zipcodes'] = lis_zip_codes
            response['message'] = 'Request successful'
            response['result'] = result
            return response, 200
        except:
            raise BadRequest('Request failed. Please try again later.')

class Refund(Resource):
    def post(self):
        response = {}
        client_email = request.form.get('client_email')
        client_message = request.form.get('client_message')
        refund_id = uuid.uuid4().hex
        if client_email == None \
          or client_message == None:
            raise BadRequest('Request failed. Please provide required details.')
        try:
            photo_key = 'refund_imgs/{}'.format(refund_id)
            photo = request.files['product_image'].read()
            photo_path = helper_upload_refund_img(photo, BUCKET_NAME, photo_key)
            todays_date = datetime.now(tz=timezone('US/Pacific')).strftime("%Y-%m-%d")
            add_meal = db.put_item(TableName='refund',
                Item={'id': {'S': refund_id},
                      'note': {'S': client_message},
                      'email': {'S': client_email},
                      'image_url': {'S': photo_path},
                      'date':{'S':todays_date}
                }
            )
            refundMsg = Message(subject='Serving Now: Refund Request',
                          sender=app.config['MAIL_USERNAME'],
                          html=render_template('refundEmailTemplate.html',
                          client_email=client_email,
                          client_message=client_message),
                          recipients=["support@servingnow.me"])
                          # recipients=["howardng940990575@gmail.com"]) # change it to customer service email when deploy
            refundMsg.attach('photo.png','image/png',photo)           
            mail.send(refundMsg)

            response['message'] = 'Request successful'
            return response, 200
        except Exception as e:
            raise BadRequest('Request failed: ' + str(e))

class Kitchen(Resource):
    def get(self, kitchen_id):
        kitchen = db.scan(TableName='kitchens',
            FilterExpression='kitchen_id = :val',
            ExpressionAttributeValues={
                ':val': {'S': kitchen_id}
            }
        )
        if (kitchen.get('Items') == []):
            return "Kitchen not found.", 404
        return kitchen, 200

    def put(self, kitchen_id):
        """ Updates kitchen information.
        Since the UI infers that a Kitchen is actually three Resources (User, Home, Kitchen),
        this method allows updates for specific Resources through the use of a 'type' key,
        which indicates which Resource is being updated.
        """
        if not kitchenExists(kitchen_id):
            return BadRequest('Kitchen could not be found.')

        response = {}
        data = request.get_json(force=True)
        if ('type' not in data):
            raise BadRequest('Missing update type.')
        if ('payload' not in data):
            raise BadRequest('Missing payload.')

        REGISTRATION_FIELD_KEYS = [
          'username',
          'password'
        ]
        PERSONAL_FIELD_KEYS = [
          'first_name',
          'last_name',
          'street',
          'city',
          'st',
          'zipcode',
          'phone_number',
          'email'
        ]
        KITCHEN_FIELD_KEYS = [
          'kitchen_name',
          'description',
          'open_time',
          'close_time',
          'delivery_option',
          'container_option',
          'cancellation_option'
        ]
        def findMissingFieldKey(fields, payload):
            """Finds the first missing field in payload.
            Returns first field in fields that is not in payload, or None if all fields are in payload.
            """
            for i in range(len(fields)):
                if fields[i] not in payload:
                    return fields[i]
            return None
        payload = data['payload']
        if (data['type'] == 'registration'):
            missing_field = findMissingFieldKey(REGISTRATION_FIELD_KEYS, payload)
            if (missing_field == None):
                try:
                    db.update_item(TableName='kitchens',
                        Key={'kitchen_id': {'S': str(kitchen_id)}},
                        UpdateExpression='SET username = :un, passsword = :pw',
                        ExpressionAttributeValues={
                            ':un': {'S': payload['username']},
                            ':pw': {'S': generate_password_hash(payload['password'])}
                        }
                    )
                    response['message'] = 'Update successful'
                    return response, 200
                except:
                    raise BadRequest('Request failed. Please try again later.')
            else:
                return BadRequest('Missing field: ' + missing_field)
        elif (data['type'] == 'personal'):
            missing_field = findMissingFieldKey(PERSONAL_FIELD_KEYS, payload)
            if (missing_field == None):
                try:
                    db.update_item(TableName='kitchens',
                        Key={'kitchen_id': {'S': str(kitchen_id)}},
                        UpdateExpression='SET first_name = :fn, last_name = :ln, street = :a, city = :c, #state = :s, zipcode = :z, phone_number = :pn, email = :e',
                        ExpressionAttributeNames={
                          '#state': 'state'
                        },
                        ExpressionAttributeValues={
                            ':fn': {'S': payload['first_name']},
                            ':ln': {'S': payload['last_name']},
                            ':a': {'S': payload['address']},
                            ':c': {'S': payload['city']},
                            ':s': {'S': payload['state']},
                            ':z': {'N': str(payload['zipcode'])},
                            ':pn': {'S': str(payload['phone_number'])},
                            ':e': {'S': payload['email']}
                        }
                    )
                    response['message'] = 'Update successful'
                    return response, 200
                except:
                    raise BadRequest('Request failed. Please try again later.')
            else:
                return BadRequest('Missing field: ' + missing_field)
        elif (data['type'] == 'kitchen'):
            missing_field = findMissingFieldKey(KITCHEN_FIELD_KEYS, payload)
            if (missing_field == None):
                try:
                    db.update_item(TableName='kitchens',
                        Key={'kitchen_id': {'S': str(kitchen_id)}},
                        UpdateExpression='SET #name = :n, description = :d, open_time = :ot, close_time = :ct, delivery_option = :do, container_option = :co, cancellation_option = :cao',
                        ExpressionAttributeNames={
                            '#name': 'name'
                        },
                        ExpressionAttributeValues={
                            ':n': {'S': payload['name']},
                            ':d': {'S': payload['description']},
                            ':ot': {'S': payload['open_time']},
                            ':ct': {'S': payload['close_time']},
                            ':do': {'S': payload['delivery_option']},
                            ':co': {'S': str(payload['container_option'])},
                            ':cao': {'S': str(payload['cancellation_option'])}
                        }
                    )
                    response['message'] = 'Update successful'
                    return response, 200
                except:
                    raise BadRequest('Request failed. Please try again later.')
            else:
                return BadRequest('Missing field: ' + missing_field)
        else:
            return BadRequest('\'type\' must have one of the following values: \'registration\', \'personal\', \'kitchen\'')


class Meals(Resource):
    def post(self, kitchen_id):
        response = {}

        kitchenFound = kitchenExists(kitchen_id)

        # raise exception if the kitchen does not exists
        if not kitchenFound:
            raise BadRequest('kitchen does not exist')

        if request.form.get('name') == None \
          or request.form.get('items') == None \
          or request.form.get('price') == None:
            raise BadRequest('Request failed. Please provide required details.')

        meal_id = uuid.uuid4().hex
        created_at = datetime.now(tz=timezone('US/Pacific')).strftime("%Y-%m-%dT%H:%M:%S")

        meal_items = json.loads(request.form['items'])

        items = []
        for i in meal_items['meal_items']:
            item = {}
            item['title'] = {}
            item['title']['S'] = i['title']
            item['qty'] = {}
            item['qty']['N'] = str(i['qty'])
            items.append(item)

        description = [{'M': i} for i in items]

        try:
            photo_key = 'meals_imgs/{}_{}'.format(str(kitchen_id), str(meal_id))
            photo_path = helper_upload_meal_img(request.files['photo'], BUCKET_NAME, photo_key)

            if photo_path == None:
                raise BadRequest('Request failed. \
                    Something went wrong uploading a photo.')

            add_meal = db.put_item(TableName='meals',
                Item={'meal_id': {'S': meal_id},
                      'created_at': {'S': created_at},
                      'kitchen_id': {'S': str(kitchen_id)},
                      'meal_name': {'S': str(request.form['name'])},
                      'description': {'L': description},
                      'price': {'S': str(request.form['price'])},
                      'photo': {'S': photo_path}
                }
            )

            kitchen = db.update_item(TableName='kitchens',
                Key={'kitchen_id': {'S': str(kitchen_id)}},
                UpdateExpression='SET isOpen = :val',
                ExpressionAttributeValues={
                    ':val': {'BOOL': True}
                }
            )

            response['message'] = 'Request successful'
            return response, 201
        except:
            raise BadRequest('Request failed. Please try again later.')

    def get(self, kitchen_id):
        response = {}

        print(kitchen_id)

        kitchenFound = kitchenExists(kitchen_id)

        # raise exception if the kitchen does not exists
        if not kitchenFound:
            raise BadRequest('kitchen does not exist')

        todays_date = datetime.now(tz=timezone('US/Pacific')).strftime("%Y-%m-%d")

        try:
            # meals = db.scan(TableName='meals',
            #     FilterExpression='kitchen_id = :value and (contains(created_at, :x1))',
            #     ExpressionAttributeValues={
            #         ':value': {'S': kitchen_id},
            #         ':x1': {'S': todays_date}
            #     }
            # )

            print("kitchen meal scan start")
            meals = db.scan(TableName='meals',
                FilterExpression='kitchen_id = :value',
                ExpressionAttributeValues={
                    ':value': {'S': kitchen_id}
                }
            )
            print("kitchen meal scan finish")


            for meal in meals['Items']:
                description = ''

                for item in meal['description']['L']:
                    if int(item['M']['qty']['N']) > 1:
                        description = description + item['M']['qty']['N'] + ' ' \
                                     + item['M']['title']['S'] + ', '
                    else:
                        description = description + item['M']['title']['S'] + ', '

                del meal['description']
                meal['description'] = {}
                meal['description']['S'] = description[:-2]

            response['message'] = 'Request successful!'
            response['result'] = meals['Items']
            return response, 200
        except:
            raise BadRequest('Request failed. Please try again later.')


class OrderReport(Resource):
    def get(self, kitchen_id):
        response = {}

        kitchenFound = kitchenExists(kitchen_id)

        # raise exception if the kitchen does not exists
        if not kitchenFound:
            raise BadRequest('kitchen does not exist')

        todays_date = datetime.now(tz=timezone('US/Pacific')).strftime("%Y-%m-%d")
        k_id = kitchen_id

        try:
            orders = db.scan(TableName='meal_orders',
                FilterExpression='kitchen_id = :value AND (contains(created_at, :x1))',
                ExpressionAttributeValues={
                    ':value': {'S': k_id},
                    ':x1': {'S': todays_date}
                }
            )

            response['result'] = orders['Items']
            response['message'] = 'Request successful'
            return response, 200
        except:
            raise BadRequest('Request failed. please try again later.')

class PaymentIntent(Resource):
    def post(self):
        response = {}
        amount = request.form.get('amount')

        if request.form.get('amount') == None:
            raise BadRequest('Request failed. Please provide the amount field.')
        try:
            amount = int(request.form.get('amount'))
        except:
            raise BadRequest('Request failed. Unable to convert amount to int')
        #print(amount)
        #Howard's Key
        #stripe.api_key = 'sk_test_MktxvO8JYzIzKIY4pgzl72f600Tt90V8bI'

        #Live test key
        stripe.api_key = app.config['STRIPE_SECRET_KEY']
        intent = stripe.PaymentIntent.create(
        amount=amount,
        currency='usd',
        )
        client_secret = intent.client_secret
        intent_id = intent.id
        response['client_secret'] = client_secret
        response['id'] = intent_id
        print(response['client_secret'])
        print(response['id'])
        return response,200

class Orders(Resource):
    def get(self):
                # orders = db.scan(AttributesToGet=["email", "phone", "name", "zipCode", "created_at"], 
                #         FilterExpression='notification_enabled == :value',
                #         ExpressionAttributeValues={':value': {'BOOL': True}},
                #         TableName="meal_orders")
        orders = db.scan(ProjectionExpression="email,phone,#full_name,zipCode,created_at,city,street,kitchen_id,order_id",
                            FilterExpression='notification_enabled = :value',
                            ExpressionAttributeValues={':value': {'BOOL': True}},
                            ExpressionAttributeNames = {'#full_name': 'name'},
                            TableName="meal_orders")
        customer_dict = {}
        for order in orders["Items"]:
            if order['email']['S'] in customer_dict:
                order['last_order_date'] = {'S':customer_dict[order['email']['S']][0]}
                order['number_of_orders'] = {'S':customer_dict[order['email']['S']][1]}
            else:
                last_order_date = db.scan(TableName="meal_orders",
                FilterExpression='email = :email',
                ExpressionAttributeValues={
                    ':email': {'S': order['email']['S']}
                })
                seq = [x['created_at']['S'] for x in last_order_date['Items']]
                last_order_date = max(seq)
                number_of_orders = len(seq)
                customer_dict[order['email']['S']] = [last_order_date,number_of_orders]
                order['last_order_date'] = {'S':last_order_date}
                order['number_of_orders'] = {'S':number_of_orders}
                # customer_dict[order['email']['S']] = last_order_date['Items'][0]['created_at']['S']
                # order['last_order_date'] = {'S':last_order_date['Items'][0]['created_at']['S']}
                # return order
        return orders,200

class SMS_Orders(Resource):
    def get(self):
                # orders = db.scan(AttributesToGet=["email", "phone", "name", "zipCode", "created_at"], 
                #         FilterExpression='notification_enabled == :value',
                #         ExpressionAttributeValues={':value': {'BOOL': True}},
                #         TableName="meal_orders")
        orders = db.scan(ProjectionExpression="email,phone,#full_name,zipCode,created_at,city,street,kitchen_id,order_id",
                            FilterExpression='notification_enabled = :value',
                            ExpressionAttributeValues={':value': {'BOOL': False}},
                            ExpressionAttributeNames = {'#full_name': 'name'},
                            TableName="meal_orders")
        none_orders = db.scan(ProjectionExpression="email,phone,#full_name,zipCode,created_at,city,street,kitchen_id,order_id",
                            FilterExpression='attribute_not_exists(notification_enabled)',
                            ExpressionAttributeNames = {'#full_name': 'name'},
                            TableName="meal_orders")    

        orders["Items"] = orders["Items"] + none_orders["Items"]

        customer_dict = {}
        for order in orders["Items"]:
            if order['email']['S'] in customer_dict:
                order['last_order_date'] = {'S':customer_dict[order['email']['S']][0]}
                order['number_of_orders'] = {'S':customer_dict[order['email']['S']][1]}
                order['ever_enabled_notification'] = {'S':customer_dict[order['email']['S']][2]}
            else:
                all_order_date = db.scan(TableName="meal_orders",
                FilterExpression='email = :email',
                ExpressionAttributeValues={
                    ':email': {'S': order['email']['S']}
                })
                seq = [x['created_at']['S'] for x in all_order_date['Items']]
                last_order_date = max(seq)
                number_of_orders = len(seq)

                ever_enabled_notification = False
                for last_order in all_order_date['Items']:
                    if 'notification_enabled' in last_order and last_order['notification_enabled']['BOOL']:
                        ever_enabled_notification = True

                customer_dict[order['email']['S']] = [last_order_date,number_of_orders,ever_enabled_notification]
                order['last_order_date'] = {'S':last_order_date}
                order['number_of_orders'] = {'S':number_of_orders}
                order['ever_enabled_notification'] = {'S':ever_enabled_notification}
        unique_customer = set()
        new_orders_items = []
        for order in orders["Items"]:
            if order['phone']['S'] not in unique_customer and order['ever_enabled_notification']['S'] is False:
                new_orders_items.append(order)
                unique_customer.add(order['phone']['S'])
        orders["Items"] = new_orders_items
        return orders,200
class Saved_Nofitication_Message(Resource):
    def put(self):
        message_name = request.form.get('message_name')
        message_payload = request.form.get('message_payload')
        if message_name is None \
          or message_payload is None:
            raise BadRequest('Request failed. Please provide required details.')

        message_id = uuid.uuid4().hex
        created_date = datetime.now(tz=timezone('US/Pacific')).strftime("%Y-%m-%dT%H:%M:%S")
        add_message = db.put_item(TableName='saved_notification_message',
                Item={'message_id': {'S': message_id},
                      'created_date': {'S': created_date},
                      'message_name': {'S': message_name},
                      'message_payload': {'S': message_payload}
                }
            )
        return 200

    def post(self):
        message_id = request.form.get('message_id')
        message_name = request.form.get('message_name')
        message_payload = request.form.get('message_payload')
        if message_id is None \
            or message_name is None \
            or message_payload is None:
            raise BadRequest('Request failed. Please provide required details.')
            
        message = db.scan(TableName='saved_notification_message',
            FilterExpression='message_id = :val',
            ExpressionAttributeValues={
                ':val': {'S': message_id}
            }
        )
        if message['Count'] is 0:
            raise BadRequest('message_id not match.')

        message = db.update_item(TableName='saved_notification_message',
                Key={'message_id': {'S': message_id}},
                UpdateExpression='SET message_name = :message_name_val, message_payload = :message_payload_val',
                ExpressionAttributeValues={
                    ':message_name_val': {'S': message_name},
                    ':message_payload_val': {'S': message_payload}
                })
        return 200

    def get(self):
        messages = db.scan(TableName="saved_notification_message")
        return messages

class Delete_Saved_Nofitication_Message(Resource): 
    def post(self):
        message_id = request.form.get('message_id')
        if message_id is None:
            raise BadRequest('message_id is required')
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('saved_notification_message')
        table.delete_item(
            Key={
                'message_id': message_id
            }
        )
        return 200

class Saved_Nofitication_Group(Resource): 
    def put(self):
        #data = {"group_name": "Test", "customers": [{'S':'02e9e8ca47274617b10f548538d427ad'},{'S':'02e9e8ca47274617b10test'}]}  
        data = request.get_json(force=True)
        if 'group_name' not in data:
            raise BadRequest('Missing group_name.')
        if 'customers' not in data:
            raise BadRequest('Missing customers.')
        group_id = uuid.uuid4().hex
        created_date = datetime.now(tz=timezone('US/Pacific')).strftime("%Y-%m-%dT%H:%M:%S")
        add_group = db.put_item(TableName='saved_notification_group',
                Item={'group_id': {'S': group_id},
                      'created_date': {'S': created_date},
                      'group_name': {'S': data['group_name']},
                      'customers': {'L': data['customers']}
                }
            )
        return 200

    def post(self):
        # data = {"group_id":"33235e1ed26e423ca549fdd04ea8e357",
        #         "group_name": "Test",
        #         "customers": [{'S':'02e9e8ca47274617b10f548538d427ad'},{'S':'02e9e8ca47274617b10test'},{'S':'one more'}]}  
        data = request.get_json(force=True)
        if 'group_id' not in data:
            raise BadRequest('Missing group_id.')
        if 'group_name' not in data:
            raise BadRequest('Missing group_name.')
        if 'customers' not in data:
            raise BadRequest('Missing customers.')

        group = db.scan(TableName='saved_notification_group',
            FilterExpression='group_id = :val',
            ExpressionAttributeValues={
                ':val': {'S': data['group_id']}
            }
        )
        if group['Count'] is 0:
            raise BadRequest('group_id not match.')
        
        group = db.update_item(TableName='saved_notification_group',
                Key={'group_id': {'S': data['group_id']}},
                UpdateExpression='SET group_name = :group_name_val, customers = :customers_val',
                ExpressionAttributeValues={
                    ':group_name_val': {'S': data['group_name']},
                    ':customers_val': {'L': data['customers']}
                })    
        return 200

    def get(self):
        groups = db.scan(TableName="saved_notification_group")
        return groups

class Delete_Saved_Nofitication_Group(Resource): 
    def post(self):
        group_id = request.form.get('group_id')
        if group_id is None:
            raise BadRequest('group_id is required')
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('saved_notification_group')
        table.delete_item(
            Key={
                'group_id': group_id
            }
        )
        return 200
class Send_Notification(Resource):
    def post(self):
        hub = NotificationHub(NOTIFICATION_HUB_KEY, NOTIFICATION_HUB_NAME, isDebug)
        tags = request.form.get('tags')
        message = request.form.get('message')
        
        if tags is None:
            raise BadRequest('Request failed. Please provide the tag field.')
        if message is None:
            raise BadRequest('Request failed. Please provide the message field.')
        tags = tags.split(',')
        for tag in tags:
            alert_payload = {
                "aps" : { 
                    "alert" : message, 
                }, 
            }
            # hub.send_apple_notification(alert_payload, tags = "default")
            hub.send_apple_notification(alert_payload, tags = tag)
            fcm_payload = {
                "data":{"message": message}
            }
            # hub.send_gcm_notification(fcm_payload, tags = "default")
            hub.send_gcm_notification(fcm_payload, tags = tag)
        return 200

class Send_Twilio_SMS(Resource):
    def post(self):
        sms_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        recipients = request.form.get('recipients')
        message = request.form.get('message')
        if not recipients:
            raise BadRequest('Request failed. Please provide the recipients field.')
        if not message:
            raise BadRequest('Request failed. Please provide the message field.')
        for destination in recipients.split(','):
            sms_client.messages.create(
                body=message,
                from_='+19254815757',
                to=destination
            )
        return 200

class Get_Registrations_From_Tag(Resource):
    def get(self, tag):
        hub = NotificationHub(NOTIFICATION_HUB_KEY, NOTIFICATION_HUB_NAME, isDebug)
        if tag is None:
            raise BadRequest('Request failed. Please provide the tag field.')
        response = hub.get_all_registrations_with_a_tag(tag)
        response = str(response.read())
        print(response)
        return response,200

class Create_or_Update_Registration_iOS(Resource):
    def post(self):
        hub = NotificationHub(NOTIFICATION_HUB_KEY, NOTIFICATION_HUB_NAME, isDebug)
        registration_id = request.form.get('registration_id')
        device_token = request.form.get('device_token')
        tags = request.form.get('tags')
        
        if tags is None:
            raise BadRequest('Request failed. Please provide the tags field.')
        if registration_id is None:
            raise BadRequest('Request failed. Please provide the registration_id field.')
        if device_token is None:
            raise BadRequest('Request failed. Please provide the device_token field.')

        response = hub.create_or_update_registration_iOS(registration_id, device_token, tags)

        return response.status

class Get_Tags_With_GUID_iOS(Resource):
    def get(self, tag):
        hub = NotificationHub(NOTIFICATION_HUB_KEY, NOTIFICATION_HUB_NAME, isDebug)
        guid = tag
        if guid is None:
            raise BadRequest('Request failed. Please provide the guid field.')
        response = hub.get_all_registrations_with_a_tag(guid)
        print(response)
        xml_response = str(response.read())[2:-1]
        # root = ET.fromstring(xml_response)
        xml_response_soup = BeautifulSoup(xml_response,features="html.parser")
        appleregistrationdescription = xml_response_soup.feed.entry.content.appleregistrationdescription
        registration_id = appleregistrationdescription.registrationid.get_text()
        device_token = appleregistrationdescription.devicetoken.get_text()
        old_tags = appleregistrationdescription.tags.get_text().split(",")
        return old_tags

class Update_Registration_With_GUID_iOS(Resource):
    def post(self):
        hub = NotificationHub(NOTIFICATION_HUB_KEY, NOTIFICATION_HUB_NAME, isDebug)
        guid = request.form.get('guid')
        tags = request.form.get('tags')
        if guid is None:
            raise BadRequest('Request failed. Please provide the guid field.')
        if tags is None:
            raise BadRequest('Request failed. Please provide the tags field.')
        response = hub.get_all_registrations_with_a_tag(guid)
        xml_response = str(response.read())[2:-1]
        # root = ET.fromstring(xml_response)
        xml_response_soup = BeautifulSoup(xml_response,features="html.parser")
        appleregistrationdescription = xml_response_soup.feed.entry.content.appleregistrationdescription
        registration_id = appleregistrationdescription.registrationid.get_text()
        device_token = appleregistrationdescription.devicetoken.get_text()
        old_tags = appleregistrationdescription.tags.get_text().split(",")
        tags = tags.split(",")
        new_tags = set(old_tags + tags)
        new_tags = ','.join(new_tags)
        print(f"tags: {old_tags}\ndevice_token: {device_token}\nregistration_id: {registration_id}")
        
        if device_token is None or registration_id is None:
            raise BadRequest('Something went wrong in retriving device_token and registration_id')
        
        response = hub.create_or_update_registration_iOS(registration_id, device_token, new_tags)
        # for type_tag in root.findall('feed/entry/content/AppleRegistrationDescription'):
        #     value = type_tag.get('Tags')
        #     print(value)
        # print("\n\n--- RESPONSE ---")
        # print(str(response.status) + " " + response.reason)
        # print(response.msg)
        # print(response.read())
        # print("--- END RESPONSE ---")
        return response.status

class Update_Registration_With_GUID_Android(Resource):
    def post(self):
        hub = NotificationHub(NOTIFICATION_HUB_KEY, NOTIFICATION_HUB_NAME, isDebug)
        guid = request.form.get('guid')
        tags = request.form.get('tags')
        if guid is None:
            raise BadRequest('Request failed. Please provide the guid field.')
        if tags is None:
            raise BadRequest('Request failed. Please provide the tags field.')
        response = hub.get_all_registrations_with_a_tag(guid)
        xml_response = str(response.read())[2:-1]
        # root = ET.fromstring(xml_response)
        xml_response_soup = BeautifulSoup(xml_response,features="html.parser")
        gcmregistrationdescription = xml_response_soup.feed.entry.content.gcmregistrationdescription
        registration_id = gcmregistrationdescription.registrationid.get_text()
        gcm_registration_id = gcmregistrationdescription.gcmregistrationid.get_text()
        old_tags = gcmregistrationdescription.tags.get_text().split(",")
        tags = tags.split(",")
        new_tags = set(old_tags + tags)
        new_tags = ','.join(new_tags)
        print(f"tags: {old_tags}\nregistration_id: {registration_id}\ngcm_registration_id: {gcm_registration_id}")
        
        if gcm_registration_id is None or registration_id is None:
            raise BadRequest('Something went wrong in retriving gcm_registration_id and registration_id')
        
        response = hub.create_or_update_registration_android(registration_id, gcm_registration_id, new_tags)
        return response.status

api.add_resource(Delete_Saved_Nofitication_Group, '/api/v1/delete_saved_notification_group')
api.add_resource(Delete_Saved_Nofitication_Message, '/api/v1/delete_saved_notification_message')
api.add_resource(Saved_Nofitication_Group, '/api/v1/saved_notification_group')
api.add_resource(Saved_Nofitication_Message, '/api/v1/saved_notification_message')
api.add_resource(Get_Tags_With_GUID_iOS, '/api/v1/get_tags_with_guid_iOS/<string:tag>')
api.add_resource(Update_Registration_With_GUID_Android, '/api/v1/update_registration_guid_android')        
api.add_resource(Update_Registration_With_GUID_iOS, '/api/v1/update_registration_guid_iOS')
api.add_resource(Get_Registrations_From_Tag, '/api/v1/get_registraions/<string:tag>')
api.add_resource(Send_Notification, '/api/v1/send_notification')
api.add_resource(Send_Twilio_SMS, '/api/v1/send_twilio_sms')
api.add_resource(Orders, '/api/v1/all_orders')
api.add_resource(SMS_Orders, '/api/v1/sms_all_orders')
api.add_resource(PaymentIntent, '/api/v1/payment')
api.add_resource(MealOrders, '/api/v1/orders')
# api.add_resource(TodaysMealPhoto, '/api/v1/meal/image/upload')
api.add_resource(RegisterKitchen, '/api/v1/kitchens/register')
api.add_resource(Meals, '/api/v1/meals/<string:kitchen_id>')
api.add_resource(OrderReport, '/api/v1/orders/report/<string:kitchen_id>')
api.add_resource(Kitchens, '/api/v1/kitchens')
api.add_resource(Kitchen, '/api/v1/kitchen/<string:kitchen_id>')
api.add_resource(Coupons, '/api/v1/coupons')
api.add_resource(Coupon, '/api/v1/coupon/<string:coupon_id>')

api.add_resource(Refund, '/api/v1/refund')

api.add_resource(ZipCodes, '/api/v1/zipcodes')
if __name__ == '__main__':
    app.run(host='localhost', port='5000')
