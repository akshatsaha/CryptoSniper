from datetime import datetime
import os
import pytz
dt = datetime.now(tz=pytz.utc)

current_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(current_dir)


print(current_dir,project_dir)