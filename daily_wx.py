import contextlib
import datetime
import pathlib
import time
from typing import Dict

from loguru import logger
from PIL import Image
import requests
from slack_sdk import WebClient


def read_secrets(path_obj: pathlib.Path) -> Dict:
    secrets = {}
    with path_obj.open('r') as f:
        for item in f.readlines():
            if item.strip().startswith('#') or item.strip() == '':
                continue
            k, v = item.split('=', 1)
            secrets[k] = v.strip()
    return secrets


def retrieve_imgs(model: str, filetype: str, span_days: int = 5):
    # Build the dates we're to lookup
    dates_str = [(TODAY - datetime.timedelta(days=x)).strftime('%Y%m%d') for x in range(span_days, 0, -1)]
    # Scan files in ./data to help with reducing redundant calls
    filenames = [x.name for x in DATA_DIR.glob(f'**/*{model}{filetype}') if x.is_file()]

    # Build out the urls to fetch. Key=filename, value=url
    url_dict = {}
    for dt in dates_str:
        # Build out two urls for each day, one at 0Z and 12Z
        for tod in ['00', '12']:
            filename = f'{dt}_{tod}0000_{model}{filetype}'
            if filename in filenames:
                logger.debug('Skipping existing file.')
                continue
            url_dict[filename] = f'{BASE_URL}/data/upper/{dt}/{filename}'

    # Retrieve the images
    for i, (filename, url) in enumerate(url_dict.items()):
        filepath = DATA_DIR.joinpath(f'{model}/{filename}')
        if i == 0:
            # Check that the filepath has all the subdirectories needed
            filepath.parent.mkdir(parents=True, exist_ok=True)
        logger.debug(f'Working on file: {filename}')
        resp = requests.get(url)
        if resp.status_code == 200:
            with filepath.open('wb') as f:
                f.write(resp.content)
            # Keep from spamming their server :)
            time.sleep(3)
        else:
            logger.warning('Response was unexpected...')
            resp.raise_for_status()


def build_gif(model: str):
    # Using ExitStack to automatically close opened images
    with contextlib.ExitStack() as stack:
        # Lazy load images
        imgs = (stack.enter_context(Image.open(f)) for f in sorted(DATA_DIR.joinpath(model).glob('**/*')))
        # Grab first image from the iterator
        img = next(imgs)
        img.save(fp=DATA_DIR.joinpath(f'gifs/{model}_{TODAY:%F}.gif'), format='GIF', append_images=imgs, save_all=True,
                 duration=300, loop=3)


def send_slack_message_blocks(props: Dict, bot_client: WebClient, model_map: Dict):
    gifs = [x for x in DATA_DIR.joinpath('gifs').glob(f'*{TODAY:%F}.gif') if x.is_file()]

    # For each gif, upload and get the URL
    for gif in gifs:
        # Extract model name from gif
        mname = '_'.join(gif.name.split('_')[:-1])
        logger.info(f'Working on model: {mname}')
        # Upload
        fupld_resp = bot_client.files_upload_v2(
            channel=props.get('private-chan'),
            file=gif.open('rb').read(),
            title=mname
        )

        if fupld_resp.data['ok']:
            # share_resp = user_client.files_sharedPublicURL(file=str(fupld_resp.data['file']['id']))
            # Get the file id
            model_map[mname]['gif-file-id'] = fupld_resp.data['file']['id']
        else:
            logger.warning('Unable to retrieve file upload info for model!')

    # Build the message block
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"WX Report for {TODAY:%F}",
                "emoji": True
            }
        }
    ]
    for mname, model_dict in model_map.items():
        # Image / GIF
        blocks.append({
            "type": "image",
            "title": {
                "type": "plain_text",
                "text": model_dict['name'],
                "emoji": True
            },
            "slack_file": {
                'id': model_dict['gif-file-id']
            },
            "alt_text": "poopie"
        })
        # Divider
        blocks.append({
            'type': 'divider'
        })

    # Waiting a few seconds reduces the change of a file-not-found error on Slack's end
    time.sleep(3)

    resp = bot_client.chat_postMessage(
        channel=props['public-chan'],
        text='Daily WX report incoming!',
        blocks=blocks
    )


ROOT = pathlib.Path(__file__).parent
SECRETS_FILE = ROOT.joinpath('secretprops.properties')
DATA_DIR = ROOT.joinpath('data')
BASE_URL = 'https://weather.ral.ucar.edu'
TODAY = datetime.datetime.today()


if __name__ == '__main__':
    props = read_secrets(SECRETS_FILE)
    bot_client = WebClient(token=props['bot-token'])
    user_client = WebClient(token=props['user-token'])

    model_map = {
        'upaCNTR_200': {'name': 'Winds @ 200mb', 'filetype': '.gif'},
        'KFWD': {'name': 'Skew-T/Log-P', 'filetype': '.png'}
    }

    for model_name, model_dict in model_map.items():
        retrieve_imgs(model=model_name, filetype=model_dict['filetype'], span_days=5)
        build_gif(model=model_name)

    send_slack_message_blocks(props=props, bot_client=bot_client, model_map=model_map)
