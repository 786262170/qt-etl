import os

from qt_etl.config import settings

schema_dir_path = os.path.join(settings.etl_save_path, 'schema')
if not os.path.exists(schema_dir_path):
    os.makedirs(schema_dir_path)
