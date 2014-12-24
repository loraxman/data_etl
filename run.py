from app import app
from app.etl import job
import os
from flask import Flask







config = os.path.join(app.root_path, 'config.cfg')
app.config.from_pyfile(config)

app.run(host="0.0.0.0", debug=True)


