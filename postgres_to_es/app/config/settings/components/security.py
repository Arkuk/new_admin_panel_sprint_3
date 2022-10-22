# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/3.2/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = os.environ.get('SECRET_KEY'),
CORS_ALLOWED_ORIGINS = ["http://127.0.0.1:8080",]
ALLOWED_HOSTS = ['127.0.0.1']
