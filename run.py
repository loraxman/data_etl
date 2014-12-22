from app import app
import os
config = os.path.join(app.root_path, 'config.cfg')
app.config.from_pyfile(config)

app.run(host="0.0.0.0", debug=True)

