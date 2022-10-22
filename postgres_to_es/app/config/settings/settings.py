import os
from pathlib import Path

from dotenv import load_dotenv
from split_settings.tools import include

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv()
DEBUG = os.environ.get('DEBUG', False) == 'True'
include(
    'components/database.py',
    'components/security.py',
    'components/application_definition.py',
    'components/password_validation.py',
    'components/internationalization.py',
    'components/static.py',
)





