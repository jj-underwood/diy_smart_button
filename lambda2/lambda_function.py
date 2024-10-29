import base64
import boto3
from boto3.dynamodb.conditions import Key
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from decimal import Decimal
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import json
import logging
import os
import pandas as pd
import pytz
import sys

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
    except Exception as e:
        logger.error(f"Config load failed: {str(e)}")
        return False

    button_devices = os.environ['BUTTON_DEVICES'].split(',')
    button_metrics = os.environ['BUTTON_METRICS'].split(',')
    env_devices = os.environ['ENV_DEVICES'].split(',')
    env_metrics = os.environ['ENV_METRICS'].split(',')
    
    dynamodb_client = boto3.resource('dynamodb')
    dynamodb_table = dynamodb_client.Table(os.environ['DYNAMODB_TABLE'])
    s3_client = boto3.client('s3')
    s3_bucket = os.environ['BUCKET_NAME']
    ses_client = boto3.client('ses')
    ses_from = os.environ['SES_FROM']
    ses_to = os.environ['SES_TO']

    tz = os.environ['TIMEZONE']

    pst = pytz.timezone(tz)
    now = datetime.now(pst)
    logger.info(f"now: {now}")
    
    day_0 = now - timedelta(days=0)
    day_1 = now - timedelta(days=1)
    day_1s = day_1.replace(hour=6, minute=0, second=0, microsecond=0)
    day_1e = day_0.replace(hour=5, minute=59, second=59, microsecond=999999)
    day_1su = day_1s.astimezone(pytz.utc)
    day_1eu = day_1e.astimezone(pytz.utc)
    day_2su = day_1su - timedelta(days=1)
    day_2eu = day_1eu - timedelta(days=1)
    logger.info(f"day_1s: {day_1s}, day_1e: {day_1e}, day_1su: {day_1su}, day_1e: {day_1eu}, day_2su: {day_2su}, day_2eu: {day_2eu}")
    week_0s = (day_0 - timedelta(days=day_0.weekday() % 7)).replace(hour=6, minute=0, second=0, microsecond=0)
    week_0e = (week_0s + timedelta(days=7)).replace(hour=5, minute=59, second=59, microsecond=999999)
    week_0su = week_0s.astimezone(pytz.utc)
    week_0eu = week_0e.astimezone(pytz.utc)
    week_1su = week_0su - timedelta(weeks=1)
    week_1eu = week_0eu - timedelta(weeks=1)
    week_2su = week_0su - timedelta(weeks=2)
    week_2eu = week_0eu - timedelta(weeks=2)
    logger.info(f"week_0s, {week_0s}, week_0e: {week_0e}, week_0su: {week_0su}, week_0eu: {week_0eu}, week_1su: {week_1su}, week_1eu: {week_1eu}, week_2su: {week_2su}, week_2eu: {week_2eu}")
    month_0s = day_0.replace(day=1, hour=6, minute=0, second=0, microsecond=0)
    month_0su = month_0s.astimezone(pytz.utc)
    month_1su = month_0su - relativedelta(months=1)
    month_1e = month_0s.replace(hour=5, minute=59, second=59, microsecond=999999)
    month_1eu = month_1e.astimezone(pytz.utc)
    month_2su = month_1su - relativedelta(months=1)
    month_2eu = month_1eu - relativedelta(months=1)
    logger.info(f"month_0s: {month_0s}, month_0su: {month_0su}, month_1su: {month_1su}, month_1eu: {month_1eu}, month_2su: {month_2su}, month_2eu: {month_2eu}")
    
    day_1_data = get_daily_data_from_dynamodb(dynamodb_table, day_1su, day_1eu, button_devices, button_metrics)
    if not day_1_data:
        return None
    
    day_1_stats = calculate_statistics('daily', tz, day_1_data, button_devices, button_metrics)
    ret = store_statistics(dynamodb_table, 'daily', day_1_stats, day_1su)
    if not ret:
        return False
    day_2_stats = get_statistics_from_dynamodb(dynamodb_table, 'daily', day_2su, day_2su, button_devices, button_metrics)
    
    logger.info(f"day_1_stats: {day_1_stats}")
    logger.info(f"day_2_stats: {day_2_stats}")
    
    week_0_stats = None
    week_1_stats = None
    if day_0.weekday() == 0:
        # Monday
        week_1_data = get_statistics_from_dynamodb(dynamodb_table, 'daily', week_1su, week_1eu, button_devices, button_metrics)
        week_1_stats = calculate_statistics('weekly', tz, week_1_data, button_devices, button_metrics)
        ret = store_statistics(dynamodb_table, 'weekly', week_1_stats, week_1su)
        if not ret:
            return False
    else:
        week_0_data = get_statistics_from_dynamodb(dynamodb_table, 'daily', week_0su, day_1eu, button_devices, button_metrics)
        week_0_stats = calculate_statistics('weekly', tz, week_0_data, button_devices, button_metrics)
        week_1_stats = get_statistics_from_dynamodb(dynamodb_table, 'weekly', week_1su, week_1su, button_devices, button_metrics)
    week_2_stats = get_statistics_from_dynamodb(dynamodb_table, 'weekly', week_2su, week_2su, button_devices, button_metrics)
    
    logger.info(f"week_0_stats: {week_0_stats}")
    logger.info(f"week_1_stats: {week_1_stats}")
    logger.info(f"week_2_stats: {week_2_stats}")
    
    month_0_stats = None
    month_1_stats = None
    if day_0.date() == month_0s.date():
        # the first day of this month
        month_1_data = get_statistics_from_dynamodb(dynamodb_table, 'daily', month_1su, month_1eu, button_devices, button_metrics)
        month_1_stats = calculate_statistics('monthly', tz, month_1_data, button_devices, button_metrics)
        ret = store_statistics(dynamodb_table, 'monthly', month_1_stats, month_1su)
        if not ret:
            return False
    else:
        month_0_data = get_statistics_from_dynamodb(dynamodb_table, 'daily', month_0su, day_1eu, button_devices, button_metrics)
        month_0_stats = calculate_statistics('monthly', tz, month_0_data, button_devices, button_metrics)
        month_1_stats = get_statistics_from_dynamodb(dynamodb_table, 'monthly', month_1su, month_1su, button_devices, button_metrics)
    month_2_stats = get_statistics_from_dynamodb(dynamodb_table, 'monthly', month_2su, month_2su, button_devices, button_metrics)
    
    logger.info(f"month_0_stats: {month_0_stats}")
    logger.info(f"month_1_stats: {month_1_stats}")
    logger.info(f"month_2_stats: {month_2_stats}")
    
    images = get_images_from_s3(config, s3_client, s3_bucket, env_devices, env_metrics, day_1, day_0)

    report = create_report(config, button_devices, day_1_stats, day_2_stats, week_0_stats, week_1_stats, week_2_stats, month_0_stats, month_1_stats, month_2_stats, images)
    ses_sub = 'Statistics ({})'.format(day_0.strftime('%Y/%m/%d'))
    ret = send_report(ses_client, report, images, ses_sub, ses_from, ses_to)

    if ret:
        ret = delete_images_from_s3(s3_client, s3_bucket, images)

    logger.info(f"ret: {ret}")

def get_daily_data_from_dynamodb(dynamodb_table, start_dt, end_dt, device_names, metrics_names):
    logger.info(f"Retrieve data from DynamoDB, start_dt: {start_dt}, end_dt: {end_dt}, devices: {device_names}, metrics: {metrics_names}")
    results = []
    total_data_size = 0
    current_dt = start_dt
    while current_dt.date() <= end_dt.date():
        date_str = current_dt.strftime('%Y-%m-%d')
        start_time = start_dt.strftime('%H:%M:%S.%f')[:-3] if current_dt.date() == start_dt.date() else '00:00:00.000'
        end_time = end_dt.strftime('%H:%M:%S.%f')[:-3] if current_dt.date() == end_dt.date() else '23:59:59.999'
        last_evaluated_key = None
        logger.info(f"date_str: {date_str}, start_time: {start_time}, end_time: {end_time}")
        while True:
            query_params = {
                'KeyConditionExpression': Key('pk').eq(date_str) & Key('sk').between(start_time, end_time)
            }
            if last_evaluated_key:
                query_params['ExclusiveStartKey'] = last_evaluated_key
            try:
                response = dynamodb_table.query(**query_params)
                items = response.get('Items', [])
                logger.info(f"dynamodb_table.query success: {len(items)} rows retrieved")
                total_data_size += sys.getsizeof(items)
            except Exception as e:
                logger.error(f"dynamodb_table.query failed: {str(e)}")
                return None
            filtered_items = filter_daily_data(items, device_names, metrics_names)
            logger.info(f"filter_daily_data: {len(filtered_items)} rows retrieved")
            results.extend(filtered_items)
            last_evaluated_key = response.get('LastEvaluatedKey')
            if not last_evaluated_key:
                break
        current_dt += timedelta(days=1)
    results.sort(key=lambda x: (x['time'], x['device']))
    logger.info(f"get_daily_data_from_dynamodb: {len(results)} rows retrieved")
    logger.info(f"Total data size retrieved: {total_data_size / 1024:.2f} KB")
    return results

def filter_daily_data(items, device_names, metrics_names):
    filtered_items = []
    for item in items:
        sk_time, sk_device = item['sk'].split('#')
        if sk_device in device_names:
            match = []
            for key in item['payload'].keys():
                if key in metrics_names:
                    match.append(key)
            if not match:
                continue
            filtered_item = {}
            for key, value in item['payload'].items():
                if key in metrics_names:
                    filtered_item['click'] = key
                    filtered_item['state'] = value
            filtered_item['time'] = datetime.strptime(' '.join([item['pk'], sk_time]), '%Y-%m-%d %H:%M:%S.%f')
            filtered_item['device'] = sk_device
            filtered_items.append(filtered_item)
    return filtered_items

def calculate_statistics(type, tzone, items, device_names, metrics_names):
    df = pd.DataFrame(items)
    logger.info(f"df: {df}")
    if type == 'daily':
        df['time'] = df['time'].dt.tz_localize('UTC')
        df['localtime'] = df['time'].dt.tz_convert(tzone)
        df['timeseconds'] = df['localtime'].dt.hour * 3600 + df['localtime'].dt.minute * 60 + df['localtime'].dt.second + (df['localtime'].dt.microsecond/1000000)
        filtered = df.loc[(df['state'] == 'on') & (df['device'].isin(device_names))]
    else:
        filtered = df.loc[(df['device'].isin(device_names))]
    logger.info(f"filtered: {filtered}")
    if type == 'daily':
        grouped = filtered.groupby(['device', 'click']).agg(count=('state', 'size'), avg_time=('timeseconds', 'mean')).reset_index()
    else:
        grouped = filtered.groupby(['device', 'click']).agg(avg_count=('count', 'mean'), avg_time=('time', 'mean')).reset_index()
    logger.info(f"grouped: {grouped}")
    df_dct = grouped.to_dict(orient='records')
    ret = []
    for device in device_names:
        for metrics in metrics_names:
            item = {'device': device, 'click': metrics, 'count': 0, 'time': 0}
            for record in df_dct:
                if record['device'] == device and record['click'] == metrics:
                    if type == 'daily':
                        item['count'] = record['count']
                        item['time'] = record['avg_time']
                    else:
                        item['count'] = record['avg_count']
                        item['time'] = record['avg_time']
            ret.append(item)
    return ret

def store_statistics(dynamodb_table, type, stats, dt):
    ret = True
    items = {}
    for stat in stats:
        if stat['device'] not in items:
            items[stat['device']] = {}
        items[stat['device']]['pk'] = '{}_stats'.format(type)
        items[stat['device']]['sk'] = '#'.join([dt.strftime('%Y-%m-%d'), stat['device']])
        if 'payload' not in items[stat['device']]:
            items[stat['device']]['payload'] = {}
        items[stat['device']]['payload'][stat['click']] = {'count': Decimal(str(stat['count'])), 'time': Decimal(str(stat['time']))}
    
    for key, value in items.items():
        try:
            response = dynamodb_table.put_item(Item = value)
            logger.info(f"dynamodb_table.put_item success: {response}")
        except Exception as e:
            logger.error(f"dynamodb_table.put_item failed: {str(e)}")
            ret = False
    return ret

def get_statistics_from_dynamodb(dynamodb_table, type, start_dt, end_dt, device_names, metrics_names):
    logger.info(f"Retrieve {type}_stats from DynamoDB, start_dt: {start_dt}, end_dt: {end_dt}, devices: {device_names}")
    results = []
    total_data_size = 0
    start_date = start_dt.strftime('%Y-%m-%d')
    end_date = end_dt.strftime('%Y-%m-%d')
    last_evaluated_key = None
    logger.info(f"start_date: {start_date}, end_date: {end_date}")
    while True:
        query_params = {
            'KeyConditionExpression': Key('pk').eq(f'{type}_stats') & Key('sk').between(f'{start_date}#', f'{end_date}#z')
        }
        if last_evaluated_key:
            query_params['ExclusiveStartKey'] = last_evaluated_key
        try:
            response = dynamodb_table.query(**query_params)
            items = response.get('Items', [])
            logger.info(f"dynamodb_table.query success: {len(items)} rows retrieved")
            total_data_size += sys.getsizeof(items)
        except Exception as e:
            logger.error(f"dynamodb_table.query failed: {str(e)}")
            return None
        filtered_items = filter_statistics(type, items, device_names, metrics_names)
        logger.info(f"filtered_statistics: {len(filtered_items)} rows retrieved")
        results.extend(filtered_items)
        last_evaluated_key = response.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break
    sorted_results = sorted(results, key=lambda x: metrics_names.index(x['click']))
    logger.info(f"get_statistics_from_dynamodb: {len(sorted_results)} rows retrieved")
    logger.info(f"Total data size retrieved: {total_data_size / 1024:.2f} KB")
    return sorted_results

def filter_statistics(type, items, device_names, metrics_names):
    filtered_items = []
    for item in items:
        sk_date, sk_device = item['sk'].split('#')
        if sk_device in device_names:
            match = []
            for key in item['payload'].keys():
                if key in metrics_names:
                    match.append(key)
            if not match:
                continue
            for key, value in item['payload'].items():
                filtered_item = {}
                if key in metrics_names:
                    filtered_item['click'] = key
                    if type == 'daily':
                        filtered_item['count'] = int(value['count'])
                    else:
                        filtered_item['count'] = float(value['count'])
                    filtered_item['time'] = float(value['time'])
                    filtered_item['date'] = sk_date
                    filtered_item['device'] = sk_device
                    filtered_items.append(filtered_item)
    return filtered_items

def get_images_from_s3(config, s3_client, bucket_name, device_names, metrics_names, start_dt, end_dt):
    images = []
    for device in device_names:
        config_device = next((d for d in config['devices'] if d.get('deviceName') == device), None)
        if not config_device:
            continue
        for metrics in metrics_names:
            config_metrics = next((d for d in config['metrics'] if d.get('metricsName') == metrics), None)
            if not config_metrics:
                continue
            filename_wo_ext = f"{device}_{metrics}_{start_dt.strftime('%Y%m%d')}-{end_dt.strftime('%Y%m%d')}"
            filename_w_ext = f"{filename_wo_ext}.png"
            try:
                response = s3_client.get_object(Bucket=bucket_name, Key=filename_w_ext)
                logger.info(f"key: {filename_w_ext}, response: {response}")
                image = {
                    "metrics": metrics,
                    "title": f"{config_device['parameters']['location']} {config_metrics['parameters']['titleName']}",
                    "filename_wo_ext": filename_wo_ext,
                    "filename_w_ext": filename_w_ext,
                    "data": response['Body'].read()
                }
            except Exception as e:
                logger.error(f"s3_client.get_object failed: {str(e)}")
                continue
            images.append(image)
    return images

def delete_images_from_s3(s3_client, s3_bucket, images):
    ret = True
    for image in images:
        try:
            response = s3_client.delete_object(Bucket=s3_bucket, Key=image['filename_w_ext'])
            logger.info(f"response: {response}")
        except Exception as e:
            logger.error(f"s3_client.delete_object failed: {str(e)}")
            ret = False
    
    return ret

def create_report(config, device_names, day_1_stats, day_2_stats, week_0_stats, week_1_stats, week_2_stats, month_0_stats, month_1_stats, month_2_stats, images):
    body_html = """<!DOCTYPE html>
    <html>
    <head>
      <style>
        body {
          font-family: 'Roboto', sans-serif;
          margin: 0;
          padding: 5px;
          background-color: #f5f5f5;
        }
        h2 {
          color: #333;
          border-bottom: 2px solid #e0e0e0;
          padding-bottom: 10px;
        }
        table {
          width: 100%;
          border-collapse: collapse;
          margin-bottom: 20px;
        }
        th, td {
          padding: 12px;
          text-align: left;
          border-bottom: 1px solid #e0e0e0;
        }
        th {
          background-color: #f5f5f5;
          color: #333;
        }
        tr:nth-child(even) {
          background-color: #f9f9f9;
        }
        .container {
          background-color: #fff;
          padding: 10px;
          border-radius: 8px;
          box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        }
      </style>
    </head>
    <body>
    <div class="container">
    """
    for device in device_names:
        config_device = next((d for d in config['devices'] if d.get('deviceName') == device), None)
        if not config_device:
            continue
        body_html += f"<h2>{config_device['parameters']['location']}</h2>"
        day_1_html = create_html_table(config, 'daily', device, day_1_stats, "Yesterday", day_2_stats)
        logger.info(f"day_1_html: {day_1_html}")
        if day_1_html != "":
            body_html += day_1_html
        week_0_html = create_html_table(config, 'weekly', device, week_0_stats, "This Week (Daily Average)", week_1_stats)
        logger.info(f"week_0_html: {week_0_html}")
        if week_0_html != "":
            body_html += week_0_html
        week_1_html = create_html_table(config, 'weekly', device, week_1_stats, "Last Week (Daily Average)", week_2_stats)
        logger.info(f"week_1_html: {week_1_html}")
        if week_1_html != "":
            body_html += week_1_html
        month_0_html = create_html_table(config, 'monthly', device, month_0_stats, "This Month (Daily Average)", month_1_stats)
        logger.info(f"month_0_html: {month_0_html}")
        if month_0_html != "":
            body_html += month_0_html
        month_1_html = create_html_table(config, 'monthly', device, month_1_stats, "Last Month (Daily Average)", month_2_stats)
        logger.info(f"month_1_html: {month_1_html}")
        if month_1_html != "":
            body_html += month_1_html
    for image in images:
        image_html = create_html_image(image)
        logger.info(f"image_html: {image_html}")
        if image_html != "":
            body_html += image_html
    body_html += """</div>
    </body>
    </html>
    """
    
    return body_html

def create_html_table(config, type, device, stats, title, previous):
    if not stats:
        return ""
    html = f"""<h3>{title}</h3>
    <table>
    """
    if type == 'daily':
        html += """<tr>
        <th>Event</th>
        <th>Count</th>
        <th></th>
        <th>Avg Time</th>
        <th></th>
        </tr>
        """
    else:
        html += """<tr>
        <th>Event</th>
        <th>Avg Count</th>
        <th></th>
        <th>Avg Time</th>
        <th></th>
        </tr>
        """
    for stat in stats:
        config_metrics = next((d for d in config['metrics'] if d.get('metricsName') == stat['click']), None)
        if not config_metrics:
            continue
        if not config_metrics['parameters']['titleName']:
            continue
        if stat['device'] != device:
            continue
        html += f"""<tr>
        <td>{config_metrics['parameters']['titleName']}</td>
        """
        target = None
        if previous:
            for item in previous:
                if stat['click'] == item['click']:
                    target = item
        if isinstance(stat['count'], int):
            html += f"""<td>{stat['count']}</td>
            """
        else:
            html += f"""<td>{stat['count']:.2f}</td>
            """
        if not target or 'count' not in target:
            html += """<td></td>
            """
        else:
            html += f"""<td>({stat['count']-target['count']:+.2f})</td>
            """
        if stat['time'] == 0:
            html += """<td></td>
            <td></td>
            """
        else:
            html += f"""<td>{(datetime.min + timedelta(seconds=stat['time'])).time().strftime('%H:%M:%S')}</td>
            """
            if not target or 'time' not in target:
                html += """<td></td>
                """
            else:
                if stat['time'] > target['time']:
                    html += f"""<td>(+{timedelta_to_hhmmss(timedelta(seconds=(stat['time']-target['time'])))})</td>
                    """
                elif stat['time'] < target['time']:
                    html += f"""<td>(-{timedelta_to_hhmmss(timedelta(seconds=(target['time']-stat['time'])))})</td>
                    """
                else:
                    html += """<td>(±00:00:00)</td>
                    """
        html += """</tr>
        """
    html += """</table>
    """
    return html

def timedelta_to_hhmmss(td):
    seconds = int(td.total_seconds())
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    return f"{hours:02}:{minutes:02}:{seconds:02}"

def create_html_image(image):
    if not image:
        return ""
    html = f"""<h3>{image['title']}</h3>
    <img src="cid:{image['filename_wo_ext']}" alt="{image['filename_w_ext']}">
    """
    return html

def send_report(ses_client, report, images, subject, from_addr, to_addr):
    logger.info(f"from_addr: {from_addr}, to_addr: {to_addr}")
    logger.info(f"subject: {subject}")
    logger.info(f"body: {report}")
    
    msg = MIMEMultipart('mixed')
    msg['Subject'] = subject
    msg['From'] = from_addr
    msg['To'] = to_addr
    msg_body = MIMEMultipart('alternative')

    part_report = MIMEText(report, 'html')

    msg_body.attach(part_report)

    msg.attach(msg_body)

    for image in images:
        part_image = MIMEBase('application', 'octet-stream')
        part_image.set_payload(image['data'])
        encoders.encode_base64(part_image)
        part_image.add_header('Content-Disposition', f"attachment; filename=\"{image['filename_w_ext']}\"")
        part_image.add_header('Content-ID', f"<{image['filename_wo_ext']}>")
        msg.attach(part_image)

    try:
        ses_client.send_raw_email(
            Source=msg['From'],
            Destinations=[msg['To']],
            RawMessage={
                'Data': msg.as_string()
            }
        )
        return True
    except Exception as e:
        logger.error(f"ses_client.send_raw_email failed: {str(e)}")
        return False

