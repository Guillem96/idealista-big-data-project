import os
import json
import base64
import requests
from requests.auth import HTTPBasicAuth

import dotenv
dotenv.load_dotenv()


def get_oauth_token(api_key, secret):
    url = "https://api.idealista.com/oauth/token"

    auth = requests.auth.HTTPBasicAuth(api_key, secret)
    body = {'grant_type': 'client_credentials'}
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
    }
    res = requests.post(url, auth=auth, headers=headers, data=body)
    return res.json()['access_token']


def dict_to_url_params(d):
    return '?' + '&'.join(f'{k}={v}' for k, v in d.items())


def should_go_to_next_page(response):
    return response['actualPage'] < response['totalPages']


class IdealistaClient(object):

    def __init__(self, 
                 api_key, 
                 secret,
                 base_path='http://api.idealista.com/3.5/es'):
        self.api_key = api_key
        self.secret = secret
        self.base_path = base_path
        self.token = get_oauth_token(self.api_key, self.secret)
        
    def search(self, **kwargs):
        headers = {'Authorization' : 'Bearer ' + self.token}
        url_params = dict_to_url_params(kwargs)
        res = requests.post(self.base_path + '/search' + url_params, 
                             headers=headers)

        if res.status_code > 300:
            print(res.text)
            raise ValueError(f'No 2xx status code. Status code: '
                             f'{res.status_code}')

        return res


if __name__ == "__main__":
    cli = IdealistaClient(os.environ['GUILLEM_API_KEY'], 
                          os.environ['GUILLEM_API_SECRET'])

    res = cli.search(
        center=LLEIDA_COORDS,
        country='es',
        distance='5000',
        operation='rent',
        maxItems=50,
        propertyType='homes',
        numPage=2)
    json_res = res.json()
    json.dump(json_res, open('test.json', 'w'))
