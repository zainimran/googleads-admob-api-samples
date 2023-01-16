# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# generate_network_report.py

import admob_utils
from google.cloud import bigquery
import json
from flatten_json import flatten
import functions_framework
import base64
import pytz
from datetime import datetime, timedelta

# Set the 'PUBLISHER_ID' which follows the format "pub-XXXXXXXXXXXXXXXX".
# See https://support.google.com/admob/answer/2784578
# for instructions on how to find your publisher ID.
# PUBLISHER_ID = 'pub-4194291010476912'


def generate_network_report(service, publisher_id):
    """Generates and prints a network report.

    Args:
      service: An AdMob Service Object.
      publisher_id: An ID that identifies the publisher.
    """

    # [START main_body]
    # Set date range. AdMob API only supports the account default timezone and
    # "America/Los_Angeles", see
    # https://developers.google.com/admob/api/v1/reference/rest/v1/accounts.networkReport/generate
    # for more information.
    tz = pytz.timezone('America/Los_Angeles')
    la_yesterday_datetime = datetime.now(tz) - timedelta(1)
    date_range = {
        'start_date': {'year': la_yesterday_datetime.year, 'month': la_yesterday_datetime.month, 'day': la_yesterday_datetime.day},
        'end_date': {'year': la_yesterday_datetime.year, 'month': la_yesterday_datetime.month, 'day': la_yesterday_datetime.day}
    }

    print(date_range)

    # Set dimensions.
    dimensions = ['DATE', 'APP', 'COUNTRY']

    # Set metrics.
    metrics = ['ESTIMATED_EARNINGS', 'IMPRESSION_RPM', 'AD_REQUESTS', 'MATCH_RATE',
               'MATCHED_REQUESTS', 'SHOW_RATE', 'IMPRESSIONS', 'IMPRESSION_CTR', 'CLICKS']

    # Set sort conditions.
    sort_conditions = {'dimension': 'DATE', 'order': 'DESCENDING'}

    # # Set dimension filters.
    # dimension_filters = {
    #     'dimension': 'AD_SOURCE',
    #     'matches_any': {5450213213286189855}
    # }

    # Create network report specifications.
    report_spec = {
        'date_range': date_range,
        'dimensions': dimensions,
        'metrics': metrics,
        'sort_conditions': [sort_conditions],
        'localization_settings': {'currency_code': 'PKR'},
        # 'dimension_filters': [dimension_filters]
    }

    # Create network report request.
    request = {'report_spec': report_spec}

    # Execute network report request.
    response = service.accounts().networkReport().generate(
        parent='accounts/{}'.format(publisher_id), body=request).execute()

    num_rows = int(response[-1]['footer']['matchingRowCount'])
    header = response[0]
    print(header)
    response = response[1:-1]

    # Flatten the json and filter out empty country rows.
    idx = 0
    while idx < num_rows:
        response[idx] = flatten(response[idx]['row'])
        if 'dimensionValues_COUNTRY' in response[idx]:
            response.pop(idx)
            idx = idx - 1
            num_rows = num_rows - 1
        idx = idx + 1

    # # Test out by saving the response to a local json file.
    # json_string = json.dumps(response)
    # with open('json_data.json', 'w') as outfile:
    #     outfile.write(json_string)

    # Construct a BigQuery client object.
    client = bigquery.Client(project='test-bigquery-play-transfer')

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = "test-bigquery-play-transfer.admob_reporting_data.admob_network_report"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, autodetect=True)

    job = client.load_table_from_json(
        response, table_id, job_config=job_config)

    job.result()  # Waits for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def admob_report_main(cloud_event):
    # # Print out the data from Pub/Sub, to prove that it worked
    print("Message: " + base64.b64decode(cloud_event.data["message"]["data"]).decode())
    publisher_ID = base64.b64decode(cloud_event.data["message"]["data"]).decode()
    print('publisher id: ', publisher_ID)
    service = admob_utils.authenticate()
    generate_network_report(service, publisher_ID)