from flask import Flask

import auth
import dashboard
import dhan
from config import secret_key

app = Flask(__name__)
app.secret_key = secret_key()
app.register_blueprint(auth.bp)
app.register_blueprint(dashboard.bp)
app.register_blueprint(dhan.bp)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False)
